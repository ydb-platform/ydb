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

## Supply-chain & security

### Docs / build hooks

`.yfm` `extensions:` run Node at docs build time (CI or local). Block unreviewed:

| Red flag | Example |
|---------|---------|
| New `extensions:` loading unknown `.cjs` | [PR #43915](https://github.com/ydb-platform/ydb/pull/43915) — `curl \| sh` in `loader.cjs` |
| `exec` / `execSync` / fetch remote script in build assets | Human review required |
| Whitespace-only diff hiding config block | Read full file |

### `.github/` workflows

- No untrusted PR head execution in `pull_request_target` without guards (see `rebase.yml` header).
- Pin actions (`actions/checkout@v5`) and pip deps in CI scripts.

## Maintenance

Update this file when changing: platform workarounds (#36673, #44180), multi-branch policy, or security review rules.

Skill router: `.cursor/rules/ci-testing.mdc` (points here for platform topics).
