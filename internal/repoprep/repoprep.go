// Package repoprep prepares isolated per-run clones and pushes their branches.
// Local and remote runners share it; bare caches accelerate fetches but are never
// shared with agent containers.
package repoprep

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"windshift/internal/redact"
)

// RepoSpec identifies a cache-scoped repository and its tokenless remote.
// Token authentication uses per-invocation GIT_ASKPASS.
type RepoSpec struct {
	WorkspaceID int
	RepoSlug    string // "owner/name" — must not contain ".." or be absolute
	RemoteURL   string // tokenless HTTPS URL
	BaseRef     string // default "main"
	Token       string // optional OAuth/PAT; askpass-injected, never embedded
	// ContinueBranch reuses a PR head; pushes remain non-force.
	ContinueBranch string
	// DestDir places multi-repo checkouts side by side; cache location is unchanged.
	DestDir string
}

// Prepared is the result of Prepare. Path is the host directory the runner
// bind-mounts as /workspace — a full, self-contained git repo. Branch is the
// run-local branch the agent commits on; BaseCommit is the SHA it was cut at.
type Prepared struct {
	Path       string
	Branch     string
	BaseCommit string
	RemoteURL  string

	// internal: retained so Push/Cleanup need not recompute.
	cacheDir string
	repoKey  string
}

// Preparer owns the on-disk root and per-repo fetch serialization. One bare
// cache per (workspace, repo); one independent clone per run beneath it.
type Preparer struct {
	rootDir      string
	gitBinary    string
	logger       *log.Logger
	allowFileURL bool

	mu        sync.Mutex
	repoLocks map[string]*sync.Mutex
}

// Options controls construction. RootDir (absolute, writable) is required.
type Options struct {
	RootDir   string
	GitBinary string
	Logger    *log.Logger
	// AllowFileURL relaxes the production ban on file:// (and bare local
	// path) remotes. Only unit tests set it, to seed an on-disk origin
	// instead of an HTTPS server. Production never does.
	AllowFileURL bool
}

// New constructs a Preparer. RootDir is not created up front (the operator
// deploys the layout) but per-repo subdirs are made lazily as runs arrive.
func New(opts Options) (*Preparer, error) {
	if opts.RootDir == "" {
		return nil, errors.New("repoprep: RootDir is required")
	}
	if !filepath.IsAbs(opts.RootDir) {
		return nil, fmt.Errorf("repoprep: RootDir must be absolute, got %q", opts.RootDir)
	}
	gitBin := opts.GitBinary
	if gitBin == "" {
		gitBin = "git"
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.Default()
	}
	return &Preparer{
		rootDir:      opts.RootDir,
		gitBinary:    gitBin,
		logger:       logger,
		allowFileURL: opts.AllowFileURL,
		repoLocks:    make(map[string]*sync.Mutex),
	}, nil
}

func (p *Preparer) lockFor(key string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()
	if l, ok := p.repoLocks[key]; ok {
		return l
	}
	l := &sync.Mutex{}
	p.repoLocks[key] = l
	return l
}

// validateBranch rejects continuation branch names that could confuse git's
// argument parsing or refspec handling. The name flows in from an external PR's
// head; a leading dash could be read as a flag and whitespace/control or refspec
// metacharacters have no place in a real branch ref. Normal branch names
// (including slashes, as in agent-runs/run-39) pass.
func validateBranch(branch string) error {
	if branch == "" {
		return errors.New("branch is required")
	}
	if strings.HasPrefix(branch, "-") {
		return fmt.Errorf("branch must not start with '-', got %q", branch)
	}
	if strings.ContainsAny(branch, " \t\n\r:?*[\\~^") {
		return fmt.Errorf("branch contains invalid characters, got %q", branch)
	}
	return nil
}

func validateRepoSlug(slug string) error {
	if slug == "" {
		return errors.New("repo slug is required")
	}
	if filepath.IsAbs(slug) {
		return fmt.Errorf("repo slug must be relative, got %q", slug)
	}
	clean := filepath.Clean(slug)
	if clean != slug || strings.Contains(slug, "..") {
		return fmt.Errorf("repo slug must not contain .. or trailing slashes, got %q", slug)
	}
	return nil
}

// Prepare fetches the cache and clones copied objects into a per-run checkout.
// Per-repository serialization prevents fetch/clone races; origin is the real remote.
func (p *Preparer) Prepare(ctx context.Context, spec RepoSpec, runID int) (*Prepared, error) {
	if spec.WorkspaceID == 0 {
		return nil, errors.New("repoprep: WorkspaceID is required")
	}
	if err := validateRepoSlug(spec.RepoSlug); err != nil {
		return nil, fmt.Errorf("repoprep: %w", err)
	}
	if spec.RemoteURL == "" {
		return nil, errors.New("repoprep: RemoteURL is required")
	}
	// A continuation run cuts the checkout on an existing remote branch (a PR
	// head) and keeps its name so the run's push lands back on the same PR; a
	// normal run cuts a fresh agent-runs/run-{id} branch from BaseRef. The ref we
	// fetch and the branch we check out coincide for a continuation, diverge
	// otherwise.
	continuation := spec.ContinueBranch != ""
	baseRef := spec.BaseRef
	if baseRef == "" {
		baseRef = "main"
	}
	fetchRef := baseRef
	if continuation {
		if err := validateBranch(spec.ContinueBranch); err != nil {
			return nil, fmt.Errorf("repoprep: continue branch: %w", err)
		}
		fetchRef = spec.ContinueBranch
	}

	repoKey := fmt.Sprintf("%d:%s", spec.WorkspaceID, spec.RepoSlug)
	repoLock := p.lockFor(repoKey)
	repoLock.Lock()
	defer repoLock.Unlock()

	repoRoot := filepath.Join(p.rootDir, fmt.Sprintf("%d", spec.WorkspaceID), spec.RepoSlug)
	cacheDir := filepath.Join(repoRoot, ".bare")
	if err := p.ensureBare(ctx, cacheDir, spec.RemoteURL, spec.Token); err != nil {
		return nil, fmt.Errorf("ensure bare cache: %w", err)
	}
	if err := p.fetchRef(ctx, cacheDir, fetchRef, spec.Token); err != nil {
		return nil, fmt.Errorf("fetch %s: %w", fetchRef, err)
	}
	baseCommit, err := p.revParse(ctx, cacheDir, fetchRef)
	if err != nil {
		return nil, fmt.Errorf("rev-parse %s: %w", fetchRef, err)
	}

	dest := filepath.Join(repoRoot, "runs", fmt.Sprintf("%d", runID))
	if spec.DestDir != "" {
		dest = spec.DestDir
	}
	// A continuation keeps the PR head's branch name so Push targets the same
	// remote branch; a normal run gets a fresh per-run branch.
	branch := fmt.Sprintf("agent-runs/run-%d", runID)
	if continuation {
		branch = spec.ContinueBranch
	}

	// Retry safety: a previous attempt may have left a partial checkout.
	if err := os.RemoveAll(dest); err != nil {
		return nil, fmt.Errorf("clear stale checkout: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o750); err != nil {
		return nil, fmt.Errorf("mkdir runs dir: %w", err)
	}

	// Copy cache objects and permit file transport only for this trusted local clone.
	if err := p.runGitLocalSource(ctx, "", "clone", "--no-hardlinks", cacheDir, dest); err != nil {
		return nil, fmt.Errorf("clone cache -> checkout: %w", err)
	}
	// Cut the run branch at the fetched base commit.
	if err := p.runGit(ctx, dest, "checkout", "-B", branch, baseCommit); err != nil {
		return nil, fmt.Errorf("checkout run branch: %w", err)
	}
	// Point origin at the real remote (tokenless) so the checkout looks
	// normal and Push targets it, not the host-local cache.
	if err := p.runGit(ctx, dest, "remote", "set-url", "origin", spec.RemoteURL); err != nil {
		return nil, fmt.Errorf("reset origin url: %w", err)
	}

	return &Prepared{
		Path:       dest,
		Branch:     branch,
		BaseCommit: baseCommit,
		RemoteURL:  spec.RemoteURL,
		cacheDir:   cacheDir,
		repoKey:    repoKey,
	}, nil
}

// Push delivers only the run branch to its real remote through PushBranch.
func (p *Preparer) Push(ctx context.Context, pr *Prepared, token string) error {
	if pr == nil {
		return errors.New("repoprep: nil prepared checkout")
	}
	_, err := PushBranch(ctx, PushOptions{
		Dest:         pr.Path,
		Branch:       pr.Branch,
		RemoteURL:    pr.RemoteURL,
		Token:        token,
		GitBinary:    p.gitBinary,
		AllowFileURL: p.allowFileURL,
		// A run in which the agent committed nothing has nothing worth
		// delivering: skip the push instead of littering the remote with a
		// branch identical to base (and a doomed PR-create call after it).
		SkipIfHeadEquals: pr.BaseCommit,
		TempRoot:         p.tempRoot(),
	})
	return err
}

// ErrNoNewCommits is returned by PushBranch when the run branch's head still
// equals SkipIfHeadEquals — the agent finished without committing (e.g. it
// answered a question via a work-item comment instead of changing code), so
// there is nothing to deliver: no branch on the remote, no PR.
var ErrNoNewCommits = errors.New("repoprep: branch head equals base — no new commits to push")

// PushOptions configures a stateless push of a single branch from an existing
// checkout — what the triage binary's `push` subcommand needs, since prepare
// and push run as separate processes that share no in-memory state.
type PushOptions struct {
	Dest         string // the per-run checkout directory
	Branch       string // run branch to push (e.g. agent-runs/run-123)
	RemoteURL    string // optional: rewrite origin before pushing (proxy transport)
	Token        string // optional: askpass token
	GitBinary    string // default "git"
	AllowFileURL bool
	// SkipIfHeadEquals, when set, short-circuits with ErrNoNewCommits if the
	// branch head resolves to this SHA (the base commit the run branch was
	// cut at): a commit-less run pushes nothing.
	SkipIfHeadEquals string
	// TempRoot, when set, is the preferred location for the sanitized push
	// repo and the askpass dir, with the system temp dir as fallback — in a
	// scratch deploy /tmp is absent or mounted noexec, and the askpass helper
	// must be executable. The in-process Preparer sets it to a subdir of its
	// worktree root; the triage binary leaves it empty because the runner
	// container always mounts an exec-capable /tmp tmpfs.
	TempRoot string
}

// PushBranch pushes exactly Branch from Dest to RemoteURL and returns the
// pushed head SHA. The push itself is performed from a fresh host-owned
// temporary repository, never from the agent-mutated checkout. That keeps
// agent-written .git/config (url.*.insteadOf, remotes, credential helpers,
// includeIf, fsmonitor, hooks, etc.) out of the credentialed push path; the
// only target used is the server-derived RemoteURL supplied by the caller.
func PushBranch(ctx context.Context, opts PushOptions) (string, error) {
	if opts.Dest == "" || opts.Branch == "" {
		return "", errors.New("repoprep: Dest and Branch are required")
	}
	if opts.RemoteURL == "" {
		return "", errors.New("repoprep: RemoteURL is required for sanitized push")
	}
	gitBin := opts.GitBinary
	if gitBin == "" {
		gitBin = "git"
	}
	destAbs, err := filepath.Abs(opts.Dest)
	if err != nil {
		return "", fmt.Errorf("abs dest: %w", err)
	}
	// The checkout is chowned to the agent uid after prepare (WI-388) while a
	// production runner pushes as root, so git's dubious-ownership check
	// rejects every operation that opens it — including the commit-less skip
	// below. The check honors safe.directory ONLY from the system/global config
	// scope when it runs inside a spawned upload-pack (the one the local fetch
	// below starts): command-scope `-c safe.directory` is silently ignored
	// there (observed on git 2.47, the runner image's version; 2.54 happens to
	// accept it, which masked this). gitOutputEnv pins GIT_CONFIG_NOSYSTEM, so
	// the only avenue left is a *global* config — hand git a throwaway one
	// carrying nothing but safe.directory so no host ~/.gitconfig leaks in.
	//
	// The check fires against two paths for the same checkout: a command that
	// discovers the repo from its working tree (rev-parse below, cwd=destAbs)
	// reports the worktree root, while the upload-pack the fetch invokes by
	// repo path reports the gitdir. List both so neither shape is rejected.
	safeCfgDir, err := mkdirTempPreferring(opts.TempRoot, "windshift-safedir-*")
	if err != nil {
		return "", fmt.Errorf("temp safe.directory config: %w", err)
	}
	defer func() { _ = os.RemoveAll(safeCfgDir) }()
	safeCfgPath := filepath.Join(safeCfgDir, "gitconfig")
	safeCfg := fmt.Sprintf("[safe]\n\tdirectory = %s\n\tdirectory = %s\n", destAbs, filepath.Join(destAbs, ".git"))
	if err := os.WriteFile(safeCfgPath, []byte(safeCfg), 0o600); err != nil {
		return "", fmt.Errorf("write safe.directory config: %w", err)
	}
	safeEnv := []string{"GIT_CONFIG_GLOBAL=" + safeCfgPath}

	shaOut, err := gitOutputEnv(ctx, gitBin, opts.AllowFileURL, destAbs, safeEnv, "rev-parse", "refs/heads/"+opts.Branch+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("rev-parse %s: %w", opts.Branch, err)
	}
	sha := strings.TrimSpace(shaOut)
	if opts.SkipIfHeadEquals != "" && sha == opts.SkipIfHeadEquals {
		return "", ErrNoNewCommits
	}

	tmp, err := mkdirTempPreferring(opts.TempRoot, "windshift-sanitized-push-*")
	if err != nil {
		return "", fmt.Errorf("temp push repo: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	if _, err := gitOutputEnv(ctx, gitBin, opts.AllowFileURL, tmp, nil, "init", "."); err != nil {
		return "", fmt.Errorf("init sanitized push repo: %w", err)
	}
	fetchSpec := fmt.Sprintf("+refs/heads/%s:refs/heads/%s", opts.Branch, opts.Branch)
	if _, err := gitOutputEnv(ctx, gitBin, true, tmp, safeEnv, "fetch", "--no-tags", "--no-recurse-submodules", destAbs, fetchSpec); err != nil {
		return "", fmt.Errorf("fetch branch into sanitized push repo: %w", err)
	}
	fetchedOut, err := gitOutputEnv(ctx, gitBin, opts.AllowFileURL, tmp, nil, "rev-parse", "refs/heads/"+opts.Branch+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("rev-parse sanitized %s: %w", opts.Branch, err)
	}
	if fetched := strings.TrimSpace(fetchedOut); fetched != sha {
		return "", fmt.Errorf("sanitized push repo fetched %s, expected %s", fetched, sha)
	}

	refspec := fmt.Sprintf("%s:refs/heads/%s", sha, opts.Branch)
	if err := gitWithToken(ctx, gitBin, opts.AllowFileURL, tmp, opts.Token, opts.TempRoot, "push", opts.RemoteURL, refspec); err != nil {
		return "", fmt.Errorf("push %s: %w", opts.Branch, err)
	}
	return sha, nil
}

// Cleanup removes the per-run checkout. Best-effort: a stale checkout is wasted
// disk, not data loss. Unlike a worktree there is no git registration to
// unwind — the clone is self-contained — so this is a plain recursive remove.
func (p *Preparer) Cleanup(_ context.Context, pr *Prepared) error {
	if pr == nil {
		return nil
	}
	repoLock := p.lockFor(pr.repoKey)
	repoLock.Lock()
	defer repoLock.Unlock()
	if err := os.RemoveAll(pr.Path); err != nil {
		p.logger.Printf("repoprep: cleanup %s: %v", pr.Path, err)
	}
	return nil
}

// RunWorkspaceDir returns the per-run parent directory a multi-repo run uses
// as the agent's working directory, with each bound repo checked out as a
// subdir beneath it (WI-449). Callers pass <dir>/<repo-dir> as RepoSpec.DestDir
// per repo, then mount this dir. CleanupWorkspaceDir removes it after the run.
func (p *Preparer) RunWorkspaceDir(runID int) string {
	return filepath.Join(p.rootDir, ".workspaces", fmt.Sprintf("run-%d", runID))
}

// CleanupWorkspaceDir removes a multi-repo run's parent workspace dir
// (WI-449). Best-effort, mirroring Cleanup.
func (p *Preparer) CleanupWorkspaceDir(runID int) {
	dir := p.RunWorkspaceDir(runID)
	if err := os.RemoveAll(dir); err != nil {
		p.logger.Printf("repoprep: cleanup workspace %s: %v", dir, err)
	}
}

// EvictIdle removes cached bare clones (and their repo trees) that have no
// active per-run checkout and whose last fetch is older than maxAge — the
// disk-hygiene backstop for the cache. Eviction takes the per-repo lock so it
// never races a Prepare. Returns the number evicted.
func (p *Preparer) EvictIdle(maxAge time.Duration, now time.Time) (int, error) {
	if p.rootDir == "" {
		return 0, nil
	}
	type bareEntry struct{ repoRoot, repoKey, barePath string }
	var found []bareEntry
	walkErr := filepath.WalkDir(p.rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d == nil {
			return nil //nolint:nilerr // skip unreadable entries and keep walking
		}
		if !d.IsDir() || d.Name() != ".bare" {
			return nil
		}
		repoRoot := filepath.Dir(path)
		rel, rerr := filepath.Rel(p.rootDir, repoRoot)
		if rerr != nil {
			return fs.SkipDir
		}
		parts := strings.SplitN(rel, string(filepath.Separator), 2)
		if len(parts) == 2 {
			found = append(found, bareEntry{repoRoot: repoRoot, repoKey: parts[0] + ":" + parts[1], barePath: path})
		}
		return fs.SkipDir // don't descend into .bare internals
	})

	evicted := 0
	for _, e := range found {
		lock := p.lockFor(e.repoKey)
		lock.Lock()
		if entries, _ := os.ReadDir(filepath.Join(e.repoRoot, "runs")); len(entries) == 0 {
			info, serr := os.Stat(filepath.Join(e.barePath, "FETCH_HEAD"))
			if serr != nil {
				info, serr = os.Stat(e.barePath)
			}
			if serr == nil && now.Sub(info.ModTime()) > maxAge {
				if rmErr := os.RemoveAll(e.repoRoot); rmErr != nil {
					p.logger.Printf("repoprep: evict %s: %v", e.repoRoot, rmErr)
				} else {
					evicted++
				}
			}
		}
		lock.Unlock()
	}
	return evicted, walkErr
}

func (p *Preparer) ensureBare(ctx context.Context, cacheDir, remoteURL, token string) error {
	if _, err := os.Stat(filepath.Join(cacheDir, "HEAD")); err == nil {
		return nil // cache already initialized
	}
	if err := os.MkdirAll(filepath.Dir(cacheDir), 0o750); err != nil {
		return fmt.Errorf("mkdir parent: %w", err)
	}
	if err := p.runGitWithToken(ctx, "", token, "clone", "--bare", remoteURL, cacheDir); err != nil {
		return fmt.Errorf("git clone --bare: %w", err)
	}
	// Disable auto-gc; manual gc is scheduled when no runs reference the repo.
	if err := p.runGit(ctx, cacheDir, "config", "gc.auto", "0"); err != nil {
		return fmt.Errorf("config gc.auto 0: %w", err)
	}
	return nil
}

func (p *Preparer) fetchRef(ctx context.Context, cacheDir, ref, token string) error {
	spec := fmt.Sprintf("+%s:%s", ref, ref)
	return p.runGitWithToken(ctx, cacheDir, token, "fetch", "--prune", "origin", spec)
}

func (p *Preparer) revParse(ctx context.Context, dir, ref string) (string, error) {
	out, err := p.runGitOutput(ctx, dir, "rev-parse", ref+"^{commit}")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

func (p *Preparer) runGit(ctx context.Context, dir string, args ...string) error {
	_, err := gitOutputEnv(ctx, p.gitBinary, p.allowFileURL, dir, nil, args...)
	return err
}

// runGitLocalSource runs git for an operation whose source is a trusted
// host-local path (the per-run clone of the bare cache), permitting the "file"
// transport that the default https-only hardening blocks. Used only where the
// source path is orchestrator-derived — never a caller- or remote-supplied URL.
func (p *Preparer) runGitLocalSource(ctx context.Context, dir string, args ...string) error {
	_, err := gitOutputEnv(ctx, p.gitBinary, true, dir, nil, args...)
	return err
}

func (p *Preparer) runGitWithToken(ctx context.Context, dir, token string, args ...string) error {
	return gitWithToken(ctx, p.gitBinary, p.allowFileURL, dir, token, p.tempRoot(), args...)
}

// tempRoot is the preferred location for the orchestrator's git scratch dirs
// (askpass helper, sanitized push repo): the worktree root is a writable,
// exec-capable volume, while a scratch deploy's /tmp is absent or noexec. It
// sits beside the per-repo trees; ".tmp" is never matched by EvictIdle's
// ".bare" walk, and each temp dir inside it is removed by its creator after
// use.
func (p *Preparer) tempRoot() string {
	return filepath.Join(p.rootDir, ".tmp")
}

func (p *Preparer) runGitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	return gitOutputEnv(ctx, p.gitBinary, p.allowFileURL, dir, nil, args...)
}

// --- package-level git plumbing (shared by Preparer and PushBranch so the
// in-process and triage-binary paths run byte-identical git) ---

// gitWithToken runs git with token injected via a per-invocation GIT_ASKPASS
// helper. The token reaches the helper through an env var only — never argv,
// never .git/config. An empty token behaves exactly like a plain run.
// tempRoot seeds mkdirTempPreferring for the askpass dir.
func gitWithToken(ctx context.Context, gitBinary string, allowFileURL bool, dir, token, tempRoot string, args ...string) error {
	if token == "" {
		_, err := gitOutputEnv(ctx, gitBinary, allowFileURL, dir, nil, args...)
		return err
	}
	dirPath, askpassPath, err := writeAskpassHelper(tempRoot)
	if err != nil {
		return fmt.Errorf("setup askpass: %w", err)
	}
	defer func() { _ = os.RemoveAll(dirPath) }()
	// GIT_TERMINAL_PROMPT / GIT_CONFIG_NOSYSTEM and the config-hardening -c
	// flags are applied centrally by gitOutputEnv; here we add only the
	// per-invocation askpass that feeds the token to git over an env var.
	_, err = gitOutputEnv(ctx, gitBinary, allowFileURL, dir, []string{
		"GIT_ASKPASS=" + askpassPath,
		"AGENT_GIT_TOKEN=" + token,
	}, args...)
	return err
}

func gitOutputEnv(ctx context.Context, gitBinary string, allowFileURL bool, dir string, extraEnv []string, args ...string) (string, error) {
	// Defense-in-depth: disable ext::/file://(unless allowed)/tar:// remote
	// helpers on every invocation so a future caller can't smuggle in a URL
	// that reaches a dangerous transport.
	fileAllow := "never"
	allowedProtocols := "https"
	if allowFileURL {
		fileAllow = "always"
		allowedProtocols = "https:file"
	}
	prefixed := append([]string{
		"-c", "protocol.ext.allow=never",
		"-c", "protocol.file.allow=" + fileAllow,
		"-c", "protocol.tar.allow=never",
		// Command-line config overrides agent-controlled hooks, credential helpers,
		// and fsmonitor processes in mutated checkouts.
		"-c", "core.hooksPath=/dev/null",
		"-c", "credential.helper=",
		"-c", "core.fsmonitor=",
	}, args...)
	// All args are operator-controlled or orchestrator-derived; no
	// user-supplied data is in scope.
	cmd := exec.CommandContext(ctx, gitBinary, prefixed...) //nolint:gosec // G204: see comment above.
	if dir != "" {
		cmd.Dir = dir
	}
	// Ignore system and inherited global config; callers may supply an isolated
	// safe.directory config. Remove inherited globals because duplicate env keys
	// can resolve to the first value.
	globalConfig := "/dev/null"
	for _, e := range extraEnv {
		if v, ok := strings.CutPrefix(e, "GIT_CONFIG_GLOBAL="); ok {
			globalConfig = v
		}
	}
	env := make([]string, 0, len(cmd.Environ())+4+len(extraEnv))
	for _, e := range cmd.Environ() {
		if strings.HasPrefix(e, "GIT_CONFIG_GLOBAL=") {
			continue // set explicitly below
		}
		env = append(env, e)
	}
	env = append(env,
		"GIT_ALLOW_PROTOCOL="+allowedProtocols,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL="+globalConfig,
		"GIT_TERMINAL_PROMPT=0",
	)
	cmd.Env = env
	cmd.Env = append(cmd.Env, extraEnv...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Never return credentials to callers that may log or persist errors.
		joined := strings.Join(args, " ")
		return redact.String(string(out)), fmt.Errorf("git %s: %w (out=%q)", joined, err, redact.String(strings.TrimSpace(string(out))))
	}
	return string(out), nil
}

// mkdirTempPreferring uses the exec-capable worktree root for Git askpass, falling back to system temp.
func mkdirTempPreferring(preferredRoot, pattern string) (string, error) {
	if preferredRoot == "" {
		return os.MkdirTemp("", pattern)
	}
	rootErr := os.MkdirAll(preferredRoot, 0o700)
	if rootErr == nil {
		dir, err := os.MkdirTemp(preferredRoot, pattern)
		if err == nil {
			return dir, nil
		}
		rootErr = err
	}
	dir, sysErr := os.MkdirTemp("", pattern)
	if sysErr != nil {
		return "", errors.Join(rootErr, sysErr)
	}
	return dir, nil
}

// writeAskpassHelper creates a private (0700) directory plus a script that
// answers git's prompts from AGENT_GIT_TOKEN. The username "oauth2" works for
// GitHub, Gitea, and GitLab (all accept any non-empty username with a token in
// the password slot). The caller removes dirPath after the git invocation.
func writeAskpassHelper(tempRoot string) (dirPath, scriptPath string, err error) {
	dirPath, err = mkdirTempPreferring(tempRoot, "windshift-askpass-*")
	if err != nil {
		return "", "", err
	}
	if err = os.Chmod(dirPath, 0o700); err != nil { //nolint:gosec // G302: dir needs +x to be traversed
		_ = os.RemoveAll(dirPath)
		return "", "", err
	}
	scriptPath = filepath.Join(dirPath, "askpass.sh")
	body := "#!/bin/sh\ncase \"$1\" in\n  Username*) printf 'oauth2\\n' ;;\n  Password*) printf '%s\\n' \"$AGENT_GIT_TOKEN\" ;;\nesac\n"
	if err = os.WriteFile(scriptPath, []byte(body), 0o700); err != nil { //nolint:gosec // G306: GIT_ASKPASS needs the exec bit
		_ = os.RemoveAll(dirPath)
		return "", "", err
	}
	return dirPath, scriptPath, nil
}
