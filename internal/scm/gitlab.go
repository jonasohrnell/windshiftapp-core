package scm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"windshift/internal/models"
)

// Compile-time check that GitLabProvider implements the optional
// interfaces it claims. See GitHubProvider / GiteaProvider for the pattern.
var (
	_ Provider                     = (*GitLabProvider)(nil)
	_ ReleaseProvider              = (*GitLabProvider)(nil)
	_ CommitProvider               = (*GitLabProvider)(nil)
	_ RefProvider                  = (*GitLabProvider)(nil)
	_ IssueCommentProvider         = (*GitLabProvider)(nil) // WI-426: drives the "@agent" PR-comment trigger via MR notes
	_ PullRequestReviewProvider    = (*GitLabProvider)(nil)
	_ RepositoryPermissionProvider = (*GitLabProvider)(nil)
	_ TokenRevoker                 = (*GitLabProvider)(nil)
)

// GitLabProvider implements the Provider interface for GitLab (gitlab.com and
// self-managed). GitLab calls repositories "projects" and pull requests "merge
// requests"; this adapter maps both onto the provider-agnostic types. The
// per-project merge-request number is its `iid`, not its global `id`.
type GitLabProvider struct {
	baseProvider
	baseURL      string
	authMethod   models.SCMAuthMethod
	accessToken  string
	clientID     string
	clientSecret string
}

// NewGitLabProvider creates a new GitLab provider instance. Unlike Gitea, an
// empty BaseURL is allowed and defaults to gitlab.com.
func NewGitLabProvider(cfg ProviderConfig) (*GitLabProvider, error) {
	baseURL := strings.TrimSuffix(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = GitLabAPIURL
	}

	var accessToken string
	switch cfg.AuthMethod {
	case models.SCMAuthMethodOAuth:
		accessToken = cfg.OAuthAccessToken
	case models.SCMAuthMethodPAT:
		accessToken = cfg.PersonalAccessToken
	}

	provider := &GitLabProvider{
		baseURL:      baseURL,
		authMethod:   cfg.AuthMethod,
		accessToken:  accessToken,
		clientID:     cfg.OAuthClientID,
		clientSecret: cfg.OAuthClientSecret,
	}
	provider.baseProvider = baseProvider{
		httpClient:          newSCMHTTPClient(30 * time.Second),
		setAuthHeader:       provider.setAuthHeader,
		handleErrorResponse: provider.handleErrorResponse,
	}

	return provider, nil
}

// GetType returns the provider type
func (g *GitLabProvider) GetType() models.SCMProviderType {
	return models.SCMProviderTypeGitLab
}

// apiURL constructs the full API v4 URL for a given path
func (g *GitLabProvider) apiURL(path string) string {
	return fmt.Sprintf("%s/api/v4%s", g.baseURL, path)
}

// projectRef URL-encodes "owner/repo" into the ":id" path segment GitLab
// expects (subgroups included: every "/" becomes "%2F").
func projectRef(owner, repo string) string {
	return url.PathEscape(owner + "/" + repo)
}

// setAuthHeader sets the bearer token. GitLab accepts both OAuth access tokens
// and personal access tokens in the Authorization: Bearer header.
func (g *GitLabProvider) setAuthHeader(req *http.Request) {
	if g.accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+g.accessToken)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
}

// handleErrorResponse maps non-success HTTP responses to sentinel errors.
func (g *GitLabProvider) handleErrorResponse(resp *http.Response) error {
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("%w: failed to read response body: %v", ErrProviderError, readErr)
	}
	bodyStr := string(body)

	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return ErrInvalidCredentials
	case http.StatusForbidden:
		if bodyStr != "" {
			return fmt.Errorf("%w: %s", ErrForbidden, bodyStr)
		}
		return ErrForbidden
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusConflict:
		return ErrAlreadyExists
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		if strings.Contains(bodyStr, "already exists") || strings.Contains(bodyStr, "already been taken") {
			return ErrAlreadyExists
		}
		return fmt.Errorf("%w: status %d - %s", ErrProviderError, resp.StatusCode, bodyStr)
	default:
		return fmt.Errorf("%w: status %d - %s", ErrProviderError, resp.StatusCode, bodyStr)
	}
}

// TestConnection verifies the credentials by reading the current user.
func (g *GitLabProvider) TestConnection(ctx context.Context) error {
	return g.doJSON(ctx, http.MethodGet, g.apiURL("/user"), http.NoBody, http.StatusOK, nil)
}

// ListRepositories lists projects the caller is a member of. When
// opts.Organization is set it lists that group's projects (subgroups included).
// opts.Search, when set, is applied server-side by GitLab so callers on
// instances with thousands of projects don't have to page through the full
// unfiltered list to find one.
func (g *GitLabProvider) ListRepositories(ctx context.Context, opts ListRepositoriesOptions) ([]Repository, error) {
	page := opts.Page
	if page == 0 {
		page = 1
	}
	limit := opts.PerPage
	if limit == 0 {
		limit = 50
	}

	q := url.Values{}
	q.Set("page", fmt.Sprintf("%d", page))
	q.Set("per_page", fmt.Sprintf("%d", limit))
	q.Set("order_by", "last_activity_at")
	if opts.Search != "" {
		// GitLab's `search` matches project name/path/description server-side;
		// search_namespaces extends that to the group/subgroup path, which
		// matters on instances with thousands of projects across many groups.
		q.Set("search", opts.Search)
		q.Set("search_namespaces", "true")
	}

	var apiPath string
	if opts.Organization != "" {
		q.Set("include_subgroups", "true")
		apiPath = fmt.Sprintf("/groups/%s/projects", url.PathEscape(opts.Organization))
	} else {
		q.Set("membership", "true")
		apiPath = "/projects"
	}
	reqURL := g.apiURL(apiPath) + "?" + q.Encode()

	var glProjects []gitlabProject
	if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &glProjects); err != nil {
		return nil, err
	}

	repos := make([]Repository, len(glProjects))
	for i, p := range glProjects {
		repos[i] = p.toRepository()
	}
	return repos, nil
}

// GetRepository gets details about a specific project.
func (g *GitLabProvider) GetRepository(ctx context.Context, owner, repo string) (*Repository, error) {
	reqURL := g.apiURL("/projects/" + projectRef(owner, repo))

	var glProject gitlabProject
	if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &glProject); err != nil {
		return nil, err
	}

	result := glProject.toRepository()
	return &result, nil
}

// ListBranches lists branches for a project, paginated up to maxBranches.
func (g *GitLabProvider) ListBranches(ctx context.Context, owner, repo string) ([]Branch, error) {
	const perPage = 100
	const maxBranches = 1000

	var branches []Branch
	for page := 1; ; page++ {
		reqURL := g.apiURL(fmt.Sprintf("/projects/%s/repository/branches?page=%d&per_page=%d",
			projectRef(owner, repo), page, perPage))

		var glBranches []gitlabBranch
		if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &glBranches); err != nil {
			return nil, err
		}
		for _, b := range glBranches {
			branches = append(branches, b.toBranch())
			if len(branches) >= maxBranches {
				return branches, nil
			}
		}
		if len(glBranches) < perPage {
			break
		}
	}
	return branches, nil
}

// glMRState translates the provider-agnostic state token into GitLab's.
func glMRState(state string) string {
	switch state {
	case "", "open":
		return "opened"
	case "all":
		return "all"
	default:
		return state // closed, merged
	}
}

// ListPullRequests lists merge requests for a project.
func (g *GitLabProvider) ListPullRequests(ctx context.Context, owner, repo string, opts ListPROptions) ([]PullRequest, error) {
	page := opts.Page
	if page == 0 {
		page = 1
	}
	limit := opts.PerPage
	if limit == 0 {
		limit = 50
	}

	q := url.Values{}
	q.Set("state", glMRState(opts.State))
	q.Set("page", fmt.Sprintf("%d", page))
	q.Set("per_page", fmt.Sprintf("%d", limit))
	switch opts.Sort {
	case "updated":
		q.Set("order_by", "updated_at")
	case "created":
		q.Set("order_by", "created_at")
	}
	if opts.Direction != "" {
		q.Set("sort", opts.Direction)
	}

	reqURL := g.apiURL(fmt.Sprintf("/projects/%s/merge_requests?%s", projectRef(owner, repo), q.Encode()))

	var glMRs []gitlabMergeRequest
	if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &glMRs); err != nil {
		return nil, err
	}

	prs := make([]PullRequest, len(glMRs))
	for i, mr := range glMRs {
		prs[i] = mr.toPullRequest()
	}
	return prs, nil
}

// GetPullRequest gets details about a specific merge request (by iid). For
// cross-project (fork) MRs it resolves the source project's full path so
// HeadRepo is populated before any push-grant decision.
func (g *GitLabProvider) GetPullRequest(ctx context.Context, owner, repo string, number int) (*PullRequest, error) {
	reqURL := g.apiURL(fmt.Sprintf("/projects/%s/merge_requests/%d", projectRef(owner, repo), number))

	var glMR gitlabMergeRequest
	if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &glMR); err != nil {
		return nil, err
	}

	pr := glMR.toPullRequest()
	if glMR.SourceProjectID != 0 && glMR.SourceProjectID != glMR.TargetProjectID {
		var srcProject gitlabProject
		if err := g.doJSON(ctx, http.MethodGet,
			g.apiURL(fmt.Sprintf("/projects/%d", glMR.SourceProjectID)),
			http.NoBody, http.StatusOK, &srcProject); err == nil {
			pr.HeadRepo = srcProject.PathWithNamespace
		}
	} else {
		pr.HeadRepo = fmt.Sprintf("%s/%s", owner, repo)
	}
	return &pr, nil
}

// ListPullRequestCommits lists commits in a merge request. Paginated up to maxPRCommits.
func (g *GitLabProvider) ListPullRequestCommits(ctx context.Context, owner, repo string, number int) ([]Commit, error) {
	const perPage = 100
	const maxPRCommits = 250

	var all []Commit
	for page := 1; ; page++ {
		reqURL := g.apiURL(fmt.Sprintf("/projects/%s/merge_requests/%d/commits?page=%d&per_page=%d",
			projectRef(owner, repo), number, page, perPage))
		var glCommits []gitlabCommit
		if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &glCommits); err != nil {
			return nil, err
		}
		for _, c := range glCommits {
			all = append(all, c.toCommit())
			if len(all) >= maxPRCommits {
				return all, nil
			}
		}
		if len(glCommits) < perPage {
			break
		}
	}
	return all, nil
}

// GetCommit gets details about a specific commit.
func (g *GitLabProvider) GetCommit(ctx context.Context, owner, repo, sha string) (*Commit, error) {
	reqURL := g.apiURL(fmt.Sprintf("/projects/%s/repository/commits/%s",
		projectRef(owner, repo), url.PathEscape(sha)))

	var glCommit gitlabCommit
	if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &glCommit); err != nil {
		return nil, err
	}

	commit := glCommit.toCommit()
	return &commit, nil
}

// ListCommits lists commits from a project branch/tag, newest first.
func (g *GitLabProvider) ListCommits(ctx context.Context, owner, repo string, opts ListCommitsOptions) ([]Commit, error) {
	page := opts.Page
	if page == 0 {
		page = 1
	}
	limit := opts.PerPage
	if limit == 0 {
		limit = 50
	}

	q := url.Values{}
	q.Set("page", fmt.Sprintf("%d", page))
	q.Set("per_page", fmt.Sprintf("%d", limit))
	if opts.Sha != "" {
		q.Set("ref_name", opts.Sha)
	}
	if opts.Since != nil && !opts.Since.IsZero() {
		q.Set("since", opts.Since.Format(time.RFC3339))
	}

	reqURL := g.apiURL(fmt.Sprintf("/projects/%s/repository/commits?%s", projectRef(owner, repo), q.Encode()))
	var glCommits []gitlabCommit
	if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &glCommits); err != nil {
		return nil, err
	}

	commits := make([]Commit, len(glCommits))
	for i, c := range glCommits {
		commits[i] = c.toCommit()
	}
	return commits, nil
}

// CreateBranch creates a new branch. GitLab passes branch/ref as query params.
func (g *GitLabProvider) CreateBranch(ctx context.Context, owner, repo, branchName, baseBranch string) error {
	q := url.Values{}
	q.Set("branch", branchName)
	q.Set("ref", baseBranch)
	createURL := g.apiURL(fmt.Sprintf("/projects/%s/repository/branches?%s", projectRef(owner, repo), q.Encode()))

	return g.doJSON(ctx, http.MethodPost, createURL, http.NoBody, http.StatusCreated, nil)
}

// CreatePullRequest opens a merge request.
func (g *GitLabProvider) CreatePullRequest(ctx context.Context, owner, repo string, opts CreatePROptions) (*PullRequest, error) {
	createURL := g.apiURL(fmt.Sprintf("/projects/%s/merge_requests", projectRef(owner, repo)))

	title := opts.Title
	if opts.Draft && !strings.HasPrefix(strings.ToLower(title), "draft:") {
		title = "Draft: " + title
	}

	body := map[string]any{
		"title":         title,
		"description":   opts.Body,
		"source_branch": opts.HeadBranch,
		"target_branch": opts.BaseBranch,
	}
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	var glMR gitlabMergeRequest
	if err := g.doJSON(ctx, http.MethodPost, createURL, strings.NewReader(string(bodyJSON)), http.StatusCreated, &glMR); err != nil {
		return nil, err
	}

	pr := glMR.toPullRequest()
	pr.HeadRepo = fmt.Sprintf("%s/%s", owner, repo)
	return &pr, nil
}

// ListIssueComments lists non-system notes on a merge request. Paginated to
// gather every note — the "@agent" PR-comment poller (WI-426) needs the full list.
func (g *GitLabProvider) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]IssueComment, error) {
	const perPage = 100
	var comments []IssueComment
	for page := 1; ; page++ {
		reqURL := g.apiURL(fmt.Sprintf("/projects/%s/merge_requests/%d/notes?page=%d&per_page=%d&sort=asc&order_by=created_at",
			projectRef(owner, repo), number, page, perPage))

		var glNotes []gitlabNote
		if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &glNotes); err != nil {
			return nil, err
		}
		for _, n := range glNotes {
			if n.System {
				continue
			}
			comments = append(comments, n.toIssueComment())
		}
		if len(glNotes) < perPage {
			break
		}
	}
	return comments, nil
}

// CreateIssueComment posts a note on a merge request and returns its id.
func (g *GitLabProvider) CreateIssueComment(ctx context.Context, owner, repo string, number int, commentBody string) (int64, error) {
	reqURL := g.apiURL(fmt.Sprintf("/projects/%s/merge_requests/%d/notes", projectRef(owner, repo), number))

	bodyJSON, err := json.Marshal(map[string]string{"body": commentBody})
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request body: %w", err)
	}

	var created gitlabNote
	if err := g.doJSON(ctx, http.MethodPost, reqURL, strings.NewReader(string(bodyJSON)), http.StatusCreated, &created); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// UpdateIssueComment is required by IssueCommentProvider but is unreachable for
// GitLab: its only caller casts to the full IssueProvider (issue↔item sync),
// which GitLab does not implement. Updating a GitLab MR note needs the parent
// merge-request iid, which this signature does not carry.
func (g *GitLabProvider) UpdateIssueComment(ctx context.Context, owner, repo string, commentID int64, commentBody string) error {
	return fmt.Errorf("%w: GitLab note update requires merge-request context", ErrProviderError)
}

// ListPullRequestReviewEvents flattens a merge request's discussions into
// normalized events. Diff notes become "review_comment" (with path/line);
// other non-empty notes become "review" bodies.
func (g *GitLabProvider) ListPullRequestReviewEvents(ctx context.Context, owner, repo string, number int) ([]IssueComment, error) {
	const perPage = 100
	var events []IssueComment
	for page := 1; ; page++ {
		reqURL := g.apiURL(fmt.Sprintf("/projects/%s/merge_requests/%d/discussions?page=%d&per_page=%d",
			projectRef(owner, repo), number, page, perPage))

		var discussions []struct {
			ID    string       `json:"id"`
			Notes []gitlabNote `json:"notes"`
		}
		if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &discussions); err != nil {
			return nil, err
		}
		for _, d := range discussions {
			for _, n := range d.Notes {
				if n.System || strings.TrimSpace(n.Body) == "" {
					continue
				}
				ev := n.toIssueComment()
				if n.Type == "DiffNote" {
					ev.Kind = "review_comment"
					ev.Path = n.Position.NewPath
					ev.Line = n.Position.NewLine
				} else {
					ev.Kind = "review"
				}
				events = append(events, ev)
			}
		}
		if len(discussions) < perPage {
			break
		}
	}
	return events, nil
}

// CanUserWriteRepository reports whether username has at least Developer
// (access_level >= 30) on the project.
func (g *GitLabProvider) CanUserWriteRepository(ctx context.Context, owner, repo, username string) (bool, error) {
	var users []gitlabUser
	if err := g.doJSON(ctx, http.MethodGet,
		g.apiURL("/users?username="+url.QueryEscape(username)),
		http.NoBody, http.StatusOK, &users); err != nil {
		return false, err
	}
	if len(users) == 0 {
		return false, nil
	}

	var member struct {
		AccessLevel int `json:"access_level"`
	}
	err := g.doJSON(ctx, http.MethodGet,
		g.apiURL(fmt.Sprintf("/projects/%s/members/all/%d", projectRef(owner, repo), users[0].ID)),
		http.NoBody, http.StatusOK, &member)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil // not a member
		}
		return false, err
	}
	return member.AccessLevel >= 30, nil
}

// CreateRelease creates a new release in a project.
func (g *GitLabProvider) CreateRelease(ctx context.Context, owner, repo string, opts CreateReleaseOptions) (*Release, error) {
	createURL := g.apiURL(fmt.Sprintf("/projects/%s/releases", projectRef(owner, repo)))

	body := map[string]any{
		"tag_name":    opts.TagName,
		"name":        opts.Name,
		"description": opts.Body,
	}
	if opts.TargetCommitish != "" {
		body["ref"] = opts.TargetCommitish
	}
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	var glRel gitlabRelease
	if err := g.doJSON(ctx, http.MethodPost, createURL, strings.NewReader(string(bodyJSON)), http.StatusCreated, &glRel); err != nil {
		return nil, err
	}

	release := glRel.toRelease()
	return &release, nil
}

// ListReleases lists releases for a project.
func (g *GitLabProvider) ListReleases(ctx context.Context, owner, repo string) ([]Release, error) {
	reqURL := g.apiURL(fmt.Sprintf("/projects/%s/releases", projectRef(owner, repo)))

	var glReleases []gitlabRelease
	if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &glReleases); err != nil {
		return nil, err
	}

	releases := make([]Release, 0, len(glReleases))
	for _, r := range glReleases {
		releases = append(releases, r.toRelease())
	}
	return releases, nil
}

// ListTags lists tags for a project, paginated up to maxTags, filtered by
// `since` against each tag's commit date.
func (g *GitLabProvider) ListTags(ctx context.Context, owner, repo string, since time.Time) ([]Tag, error) {
	const perPage = 100
	const maxTags = 500

	var tags []Tag
	for page := 1; ; page++ {
		reqURL := g.apiURL(fmt.Sprintf("/projects/%s/repository/tags?page=%d&per_page=%d",
			projectRef(owner, repo), page, perPage))

		var raw []gitlabTag
		if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &raw); err != nil {
			return nil, err
		}
		for _, t := range raw {
			created := t.Commit.CommittedDate
			if created.IsZero() {
				created = t.Commit.CreatedAt
			}
			if !since.IsZero() && created.Before(since) {
				continue
			}
			tags = append(tags, Tag{Name: t.Name, SHA: t.Target, CreatedAt: created})
			if len(tags) >= maxTags {
				return tags, nil
			}
		}
		if len(raw) < perPage {
			break
		}
	}
	return tags, nil
}

// CompareCommits returns commits reachable from `head` but not `base`, oldest
// first. GitLab's /compare takes from=base, to=head.
func (g *GitLabProvider) CompareCommits(ctx context.Context, owner, repo, base, head string) ([]Commit, error) {
	const maxCompareCommits = 500

	q := url.Values{}
	q.Set("from", base)
	q.Set("to", head)
	reqURL := g.apiURL(fmt.Sprintf("/projects/%s/repository/compare?%s", projectRef(owner, repo), q.Encode()))

	var resp struct {
		Commits []gitlabCommit `json:"commits"`
	}
	if err := g.doJSON(ctx, http.MethodGet, reqURL, http.NoBody, http.StatusOK, &resp); err != nil {
		return nil, err
	}

	out := make([]Commit, 0, len(resp.Commits))
	for _, c := range resp.Commits {
		out = append(out, c.toCommit())
		if len(out) >= maxCompareCommits {
			break
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// RegisterWebhook registers a project hook. GitLab models subscriptions as
// individual boolean flags rather than an events array.
func (g *GitLabProvider) RegisterWebhook(ctx context.Context, owner, repo string, opts WebhookOptions) (*WebhookRegistration, error) {
	createURL := g.apiURL(fmt.Sprintf("/projects/%s/hooks", projectRef(owner, repo)))

	pushEvents, mrEvents, tagEvents := false, false, false
	if len(opts.Events) == 0 {
		pushEvents, mrEvents = true, true
	}
	for _, e := range opts.Events {
		switch e {
		case "push":
			pushEvents = true
		case "pull_request", "merge_request", "merge_requests":
			mrEvents = true
		case "tag", "tag_push", "create":
			tagEvents = true
		}
	}

	body := map[string]any{
		"url":                     opts.URL,
		"token":                   opts.Secret,
		"push_events":             pushEvents,
		"merge_requests_events":   mrEvents,
		"tag_push_events":         tagEvents,
		"enable_ssl_verification": true,
	}
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	var hook gitlabHook
	if err := g.doJSON(ctx, http.MethodPost, createURL, strings.NewReader(string(bodyJSON)), http.StatusCreated, &hook); err != nil {
		return nil, err
	}

	var events []string
	if hook.PushEvents {
		events = append(events, "push")
	}
	if hook.MergeRequestsEvents {
		events = append(events, "pull_request")
	}
	if hook.TagPushEvents {
		events = append(events, "tag")
	}

	return &WebhookRegistration{
		ID:        fmt.Sprintf("%d", hook.ID),
		URL:       hook.URL,
		Events:    events,
		IsActive:  true,
		CreatedAt: hook.CreatedAt,
	}, nil
}

// DeleteWebhook removes a project hook.
func (g *GitLabProvider) DeleteWebhook(ctx context.Context, owner, repo, webhookID string) error {
	deleteURL := g.apiURL(fmt.Sprintf("/projects/%s/hooks/%s", projectRef(owner, repo), url.PathEscape(webhookID)))
	return g.doJSON(ctx, http.MethodDelete, deleteURL, http.NoBody, http.StatusNoContent, nil)
}

// =============================================================================
// GitLab API response types
// =============================================================================

type gitlabProject struct {
	ID                int64     `json:"id"`
	Name              string    `json:"name"`
	Path              string    `json:"path"`
	PathWithNamespace string    `json:"path_with_namespace"`
	Description       string    `json:"description"`
	WebURL            string    `json:"web_url"`
	HTTPURLToRepo     string    `json:"http_url_to_repo"`
	SSHURLToRepo      string    `json:"ssh_url_to_repo"`
	DefaultBranch     string    `json:"default_branch"`
	Visibility        string    `json:"visibility"`
	Archived          bool      `json:"archived"`
	CreatedAt         time.Time `json:"created_at"`
	LastActivityAt    time.Time `json:"last_activity_at"`
	Namespace         struct {
		FullPath string `json:"full_path"`
		Path     string `json:"path"`
	} `json:"namespace"`
}

func (p gitlabProject) toRepository() Repository {
	owner := p.Namespace.FullPath
	if owner == "" && p.PathWithNamespace != "" {
		if i := strings.LastIndex(p.PathWithNamespace, "/"); i >= 0 {
			owner = p.PathWithNamespace[:i]
		}
	}
	return Repository{
		ID:            fmt.Sprintf("%d", p.ID),
		Name:          p.Name,
		FullName:      p.PathWithNamespace,
		Description:   p.Description,
		URL:           p.WebURL,
		CloneURL:      p.HTTPURLToRepo,
		SSHURL:        p.SSHURLToRepo,
		DefaultBranch: p.DefaultBranch,
		IsPrivate:     p.Visibility != "public",
		IsArchived:    p.Archived,
		Owner:         owner,
		CreatedAt:     p.CreatedAt,
		UpdatedAt:     p.LastActivityAt,
	}
}

type gitlabBranch struct {
	Name      string `json:"name"`
	Default   bool   `json:"default"`
	Protected bool   `json:"protected"`
	Commit    struct {
		ID string `json:"id"`
	} `json:"commit"`
}

func (b gitlabBranch) toBranch() Branch {
	return Branch{
		Name:      b.Name,
		SHA:       b.Commit.ID,
		IsDefault: b.Default,
		Protected: b.Protected,
	}
}

type gitlabMergeRequest struct {
	ID              int64      `json:"id"`
	IID             int64      `json:"iid"`
	Title           string     `json:"title"`
	Description     string     `json:"description"`
	State           string     `json:"state"` // opened, closed, merged, locked
	WebURL          string     `json:"web_url"`
	SourceBranch    string     `json:"source_branch"`
	TargetBranch    string     `json:"target_branch"`
	SHA             string     `json:"sha"`
	Draft           bool       `json:"draft"`
	WorkInProgress  bool       `json:"work_in_progress"`
	SourceProjectID int64      `json:"source_project_id"`
	TargetProjectID int64      `json:"target_project_id"`
	Author          gitlabUser `json:"author"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	MergedAt        *time.Time `json:"merged_at"`
	ClosedAt        *time.Time `json:"closed_at"`
}

func (mr gitlabMergeRequest) toPullRequest() PullRequest {
	state := mr.State
	switch mr.State {
	case "opened", "locked":
		state = "open"
	case "merged":
		state = "merged"
	case "closed":
		state = "closed"
	}

	return PullRequest{
		ID:         int(mr.ID),
		Number:     int(mr.IID),
		Title:      mr.Title,
		Body:       mr.Description,
		State:      state,
		URL:        mr.WebURL,
		HeadBranch: mr.SourceBranch,
		HeadSHA:    mr.SHA,
		BaseBranch: mr.TargetBranch,
		IsMerged:   mr.State == "merged",
		IsDraft:    mr.Draft || mr.WorkInProgress,
		Author:     mr.Author.toUser(),
		CreatedAt:  mr.CreatedAt,
		UpdatedAt:  mr.UpdatedAt,
		MergedAt:   mr.MergedAt,
		ClosedAt:   mr.ClosedAt,
	}
}

type gitlabCommit struct {
	ID             string    `json:"id"`
	Message        string    `json:"message"`
	Title          string    `json:"title"`
	WebURL         string    `json:"web_url"`
	AuthorName     string    `json:"author_name"`
	AuthorEmail    string    `json:"author_email"`
	AuthoredDate   time.Time `json:"authored_date"`
	CommitterName  string    `json:"committer_name"`
	CommitterEmail string    `json:"committer_email"`
	CommittedDate  time.Time `json:"committed_date"`
	CreatedAt      time.Time `json:"created_at"`
}

func (c gitlabCommit) toCommit() Commit {
	webURL := c.WebURL
	message := c.Message
	if message == "" {
		message = c.Title
	}
	createdAt := c.AuthoredDate
	if createdAt.IsZero() {
		createdAt = c.CreatedAt
	}
	return Commit{
		SHA:       c.ID,
		Message:   message,
		URL:       webURL,
		Author:    User{Name: c.AuthorName, Email: c.AuthorEmail},
		Committer: User{Name: c.CommitterName, Email: c.CommitterEmail},
		CreatedAt: createdAt,
	}
}

type gitlabUser struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	AvatarURL string `json:"avatar_url"`
}

func (u gitlabUser) toUser() User {
	name := u.Name
	if name == "" {
		name = u.Username
	}
	return User{
		ID:        fmt.Sprintf("%d", u.ID),
		Username:  u.Username,
		Name:      name,
		Email:     u.Email,
		AvatarURL: u.AvatarURL,
	}
}

type gitlabRelease struct {
	TagName     string     `json:"tag_name"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	CreatedAt   time.Time  `json:"created_at"`
	ReleasedAt  *time.Time `json:"released_at"`
	Upcoming    bool       `json:"upcoming_release"`
	Links       struct {
		Self string `json:"self"`
	} `json:"_links"`
}

func (r gitlabRelease) toRelease() Release {
	return Release{
		ID:           r.TagName, // GitLab releases have no numeric id; the tag is the key
		TagName:      r.TagName,
		Name:         r.Name,
		Body:         r.Description,
		URL:          r.Links.Self,
		IsDraft:      false,
		IsPrerelease: r.Upcoming,
		CreatedAt:    r.CreatedAt,
		PublishedAt:  r.ReleasedAt,
	}
}

type gitlabTag struct {
	Name   string `json:"name"`
	Target string `json:"target"`
	Commit struct {
		ID            string    `json:"id"`
		CreatedAt     time.Time `json:"created_at"`
		CommittedDate time.Time `json:"committed_date"`
	} `json:"commit"`
}

type gitlabNote struct {
	ID        int64      `json:"id"`
	Body      string     `json:"body"`
	Author    gitlabUser `json:"author"`
	System    bool       `json:"system"`
	Type      string     `json:"type"` // "", "DiffNote", "DiscussionNote"
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	Position  struct {
		NewPath string `json:"new_path"`
		NewLine int    `json:"new_line"`
	} `json:"position"`
}

func (n gitlabNote) toIssueComment() IssueComment {
	return IssueComment{
		ID:        n.ID,
		Kind:      "issue_comment",
		Body:      n.Body,
		User:      n.Author.toUser(),
		CreatedAt: n.CreatedAt,
		UpdatedAt: n.UpdatedAt,
	}
}

type gitlabHook struct {
	ID                  int64     `json:"id"`
	URL                 string    `json:"url"`
	PushEvents          bool      `json:"push_events"`
	MergeRequestsEvents bool      `json:"merge_requests_events"`
	TagPushEvents       bool      `json:"tag_push_events"`
	CreatedAt           time.Time `json:"created_at"`
}

// =============================================================================
// OAuth methods
// =============================================================================

// performTokenRequest performs an OAuth token exchange against GitLab's token
// endpoint. GitLab rotates refresh tokens on every refresh and returns
// `invalid_grant` when a stale/consumed refresh token is presented — a terminal
// condition mapped to ErrRefreshTokenInvalid so the caller marks the credential
// dead and prompts a fresh connect.
func (g *GitLabProvider) performTokenRequest(ctx context.Context, params url.Values) (*OAuthTokens, error) {
	tokenURL := fmt.Sprintf("%s/oauth/token", g.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := g.httpClient.Do(req) //nolint:gosec // URL from admin-configured GitLab baseURL
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("%w: failed to read response body: %v", ErrProviderError, readErr)
	}

	if resp.StatusCode != http.StatusOK {
		if bytes.Contains(body, []byte(`"invalid_grant"`)) {
			return nil, fmt.Errorf("%w: %s", ErrRefreshTokenInvalid, string(body))
		}
		return nil, fmt.Errorf("%w: %s", ErrProviderError, string(body))
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		TokenType    string `json:"token_type"`
		RefreshToken string `json:"refresh_token,omitempty"`
		ExpiresIn    int    `json:"expires_in,omitempty"`
		Scope        string `json:"scope,omitempty"`
		Error        string `json:"error,omitempty"`
		ErrorDesc    string `json:"error_description,omitempty"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, err
	}

	if tokenResp.Error == "invalid_grant" {
		return nil, fmt.Errorf("%w: %s", ErrRefreshTokenInvalid, tokenResp.ErrorDesc)
	}
	if tokenResp.Error != "" {
		return nil, fmt.Errorf("%w: %s - %s", ErrProviderError, tokenResp.Error, tokenResp.ErrorDesc)
	}

	tokens := &OAuthTokens{
		AccessToken:  tokenResp.AccessToken,
		TokenType:    tokenResp.TokenType,
		RefreshToken: tokenResp.RefreshToken,
		Scope:        tokenResp.Scope,
	}
	if tokenResp.ExpiresIn > 0 {
		expiresAt := time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
		tokens.ExpiresAt = &expiresAt
	}
	return tokens, nil
}

// ExchangeCode exchanges an OAuth authorization code for access tokens.
func (g *GitLabProvider) ExchangeCode(ctx context.Context, code, redirectURI string) (*OAuthTokens, error) {
	params := url.Values{
		"client_id":     {g.clientID},
		"client_secret": {g.clientSecret},
		"code":          {code},
		"grant_type":    {"authorization_code"},
		"redirect_uri":  {redirectURI},
	}
	return g.performTokenRequest(ctx, params)
}

// RefreshToken refreshes an expired access token using a refresh token.
func (g *GitLabProvider) RefreshToken(ctx context.Context, refreshToken string) (*OAuthTokens, error) {
	params := url.Values{
		"client_id":     {g.clientID},
		"client_secret": {g.clientSecret},
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
	}
	return g.performTokenRequest(ctx, params)
}

// GetCurrentUser returns the authenticated user's info from GitLab.
func (g *GitLabProvider) GetCurrentUser(ctx context.Context) (*User, error) {
	var glUser gitlabUser
	if err := g.doJSON(ctx, http.MethodGet, g.apiURL("/user"), http.NoBody, http.StatusOK, &glUser); err != nil {
		return nil, err
	}
	user := glUser.toUser()
	return &user, nil
}

// RevokeToken asks GitLab to invalidate an OAuth access token. Implements
// scm.TokenRevoker; disconnect handlers call this best-effort.
func (g *GitLabProvider) RevokeToken(ctx context.Context, accessToken string) error {
	params := url.Values{
		"client_id":     {g.clientID},
		"client_secret": {g.clientSecret},
		"token":         {accessToken},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/oauth/revoke", g.baseURL), strings.NewReader(params.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := g.httpClient.Do(req) //nolint:gosec // URL from admin-configured GitLab baseURL
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: status %d - %s", ErrProviderError, resp.StatusCode, string(body))
	}
	return nil
}
