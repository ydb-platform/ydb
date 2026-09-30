# CI Platform: GitHub Actions semantics & safety

Canonical reference for **GitHub platform behavior**, **multi-branch CI**, and **supply-chain review** when changing `.github/`.

Related: test pipeline map → [CI_PIPELINE.md](CI_PIPELINE.md) · analytics → [ARCHITECTURE.md](../scripts/analytics/ARCHITECTURE.md)

**Re-read official docs before workflow changes:** [Events](https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows) · [`pull_request_target`](https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows#pull_request_target) · [Workflow syntax](https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions) · [`schedule`](https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows#schedule)

## Known YDB traps (production-verified)

| Trap | What happens | Mitigation |
|------|----------------|------------|
| **`pull_request_target` workflow from default branch** | PR into `stable-*` runs workflow YAML from **`main`**, not target branch. Merging CI changes to `main` affects all base branches immediately. | Backport plan or backward-compatible action inputs; see [Multi-branch](#multi-branch--shared-actions). |
| **Stale `merge_commit_sha`** | `GET /pulls/{N}` and workflow `context.payload.pull_request` can return outdated synthetic merge SHA — CI tests code merged with old base. | [#36673](https://github.com/ydb-platform/ydb/issues/36673): re-fetch PR in gate; `prepare-merge` / `refs/pr-ci/{N}/merge`; don't trust cached SHA alone. |
| **`rebase-and-check` label** | Re-triggers PR-check; context PR object may still be stale until API refresh. | Same as merge_commit_sha. |
| **`schedule` cron is best-effort** | Missed or delayed runs during outages. | [PR #44180](https://github.com/ydb-platform/ydb/pull/44180): `automerge_pr.yaml` cron at `:17` + **~55 min in-job poll loop**. |
| **Automerge push race** | Base moves between clone and push → false `automerge-blocked`. | `automerge.py` `refresh_base_and_merge()` — fetch `origin/<base>` immediately before merge. |

When proposing fixes, cite the GitHub doc section you rely on (or note undocumented quirk + existing workaround).

## Multi-branch & shared actions

Composite actions (`build_and_test_ya`, `ya_ci_core_build_test`, …) may differ across `main` vs `stable-*` / `q-stable-*`. Workflow YAML on a branch must match action API **on that branch**.

### Checklist (`.github/actions/**` or caller workflows)

1. Find callers: `rg 'uses:.*\.github/actions/' .github/workflows .github/actions`
2. **Breaking change?** → backward-compatible inputs, or update all callers same PR, or document cherry-picks to active stable branches.
3. **`pull_request_target` on stable PRs** uses **main's** workflow file — new action on `main` applies even when base is `stable-25-4`.
4. **Cherry-picks:** `.github/` on target branch must match action version there.
5. **Fork PRs:** workflow from base repo; checkout ref from gate / merge commit — see [CI_PIPELINE.md § PR-check](CI_PIPELINE.md#pr-check-specifics).

### PR description template (shared CI changes)

```markdown
**CI layer:** Trigger | Gate | Orchestrator | composite action | test_ya | script
**Consumers:** workflows calling this action
**Branches:** main only | backport to stable-* (list)
**GitHub semantics:** e.g. pull_request_target from main, merge_commit_sha refresh
**Docs:** CI_PIPELINE.md | CI_PLATFORM.md | ARCHITECTURE.md (if job_name)
```

## Security at development & review

`.github/` changes run on shared infrastructure with elevated tokens. **Do not introduce execution paths for untrusted or unreviewed code.**

Principles (apply to workflows, composite actions, CI scripts, and build hooks — docs, release, etc.):

- **Least privilege:** minimal `permissions`, no broad secrets in fork-exposed jobs.
- **No untrusted execution:** under `pull_request_target`, do not run PR head code without the same guards as `rebase.yml` (collaborator checks, no arbitrary build of fork code).
- **No remote code at runtime:** avoid `curl | sh`, unpinned downloads, `exec`/`eval` on external input in CI and build pipelines.
- **Pin dependencies:** action versions (`actions/checkout@v5`), pip/npm packages in CI scripts.
- **Review the whole diff:** cosmetic-only changes can hide new steps, hooks, or config blocks — read full files for security-sensitive paths.
- **Question new side effects:** anything that runs on schedule, on every PR, or at build time affects all maintainers — treat as high scrutiny.

When in doubt, ask for a second human review before merge.

## Secrets & tokens in workflows

How we work with credentials in `.github/` — **avoid leaks, never publish tokens in logs or artifacts**.

### Secrets vs vars vs `github.token`

| Kind | Store | Typical use | Logged in Actions UI? |
|------|--------|-------------|------------------------|
| **Secrets** (`secrets.*`) | Encrypted repo/org secrets | PAT, YDB SA JSON, AWS keys, bot tokens | Masked if printed verbatim |
| **Variables** (`vars.*`) | Plain repo variables | `CHECKS_SWITCH`, `YDB_QA_CONFIG`, bucket names | **Yes** — not for credentials |
| **`github.token`** | Ephemeral per job | PR comments, statuses, API from trusted steps | Masked; scoped by job `permissions` |

**Rule:** passwords, keys, SA JSON → **secrets only**. Config JSON without private keys may live in `vars` (e.g. `YDB_QA_CONFIG` table paths).

### Do

- Set **`permissions:`** at workflow/job level to the minimum needed (read vs write contents, pull-requests, etc.).
- Pass secrets via **`env:`** or **file paths** (`mktemp` + write, then pass path) — pattern in `test_ya` (telegram token file) and `setup_ci_ydb_service_account_key_file_credentials` (SA JSON → `/tmp/...`, not stdout).
- Use composite-action **`format('{{"KEY":"{0}"}}', secrets.X)`** JSON blobs for bundled secrets (see `build_and_test_ya` callers) — avoids echo in workflow YAML steps.
- Keep **`pull_request_target`** jobs from running **PR head scripts** with access to secrets; gate fork PRs (`ok-to-test`, collaborator checks) before any step that uses `secrets.*`.
- Use **`github.token`** for PR comments/statuses where enough; reserve PAT/`YDBOT_TOKEN` for operations default token cannot do.
- Redact before debug: never `echo`, `print`, `curl -v`, or `set -x` on lines containing secrets; avoid logging full `env` in CI scripts.
- Keep local credentials **gitignored** (`.cursor/mcp.json`, SA key files) — never commit.

### Don't

- **`echo ${{ secrets.* }}`** or interpolate secrets into run URLs, issue bodies, S3 public paths, or PR comments.
- Put secrets in **`vars`**, workflow outputs, or **`GITHUB_ENV`** values that later get printed.
- Rely on GitHub masking for **constructed** secrets (concat, base64, substring) — masking may not apply.
- Pass secrets into steps that run **untrusted code** from PR head (fork) without the same model as `rebase.yml`.
- Log **`YDB_QA_CONFIG`** / env dumps in analytics scripts in CI without checking for embedded credentials (config should be paths/endpoints only).
- Commit **example** workflow files with real tokens (use placeholders; template: `mcp.ydb-qa.example.json`).

### Review checklist (secrets)

```markdown
- [ ] No new secret in vars / plain text in repo
- [ ] No echo/print/log of secrets or env containing secrets
- [ ] pull_request_target: secrets only in trusted steps after fork/collaborator gate
- [ ] Job permissions minimal for what the step needs
- [ ] New PAT/bot token scoped narrowly; existing secret reused if possible
```

Ref: [GitHub encrypted secrets](https://docs.github.com/en/actions/security-for-github-actions/security-guides/using-secrets-in-github-actions) · [Workflow permissions](https://docs.github.com/en/actions/using-jobs/assigning-permissions-to-jobs)

## Maintenance

Update this file when changing: platform workarounds (#36673, #44180), multi-branch policy, or security review expectations.

Skill router: `.cursor/rules/ci-testing.mdc` (points here for platform topics).
