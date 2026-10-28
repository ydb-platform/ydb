---
name: generate-internal-changelog
description: >-
  Use when a user asks to prepare or make an internal YDB release, including
  requests such as "сделай внутренний релиз", "подготовь внутренний релиз",
  or "internal release", and for YDB Server feature release notes tied to
  YDBFEATURES and stable-X-Y branches.
---

# Generate internal YDB Server release notes

## Overview

Prepare feature-only bilingual notes for an internal release candidate. The
exact RC tag is the authoritative boundary for implementation, flag defaults,
and artifacts. Tracker release fields produce candidates, but never prove that
a feature is included, enabled by default, or ready for public wording.

An RC such as `26.3.1.16` is published as release line `26.3 RC`. Its patch
components identify the exact code tag and Tracker value, but do not appear in
a public changelog version.

## Non-negotiable invariants

For RC tag `26.3.1.16`:

| Item | Required value |
|---|---|
| Public heading and anchor | `Version 26.3 RC {#26-3-rc}` |
| RU heading and anchor | `Версия 26.3 RC {#26-3-rc}` |
| Exact implementation ref | Git tag `26.3.1.16` |
| Code branch to verify tag containment | `origin/stable-26-3-1` |
| Original release-notes PR base | `main` |
| Docs selector | `?version=v26.3` |
| Tracker code-ready/default value | `stable-26-3-1` |

Resolve the annotated tag to its commit and prove it is reachable from the
matching `stable-X-Y-1` branch. Do not substitute the moving branch tip for
the tag. Do not use `stable-26-3` for a `26.3.1.N` RC, and do not write
`26.3.1.16` in public headings, anchors, or documentation selectors.

This workflow changes only:

- `ydb/docs/en/core/changelog-server.md`
- `ydb/docs/ru/core/changelog-server.md`
- `ydb/docs/en/core/downloads/ydb-open-source-database.md`
- `ydb/docs/ru/core/downloads/ydb-open-source-database.md`

Do not publish a Bug Fixes section. In both downloads files, add the exact RC
row to Linux, Docker, and Source Code tables under the `v26.3 RC` group above
the preceding release line. The changelog link must use the RC anchor. Never
label an RC downloads group as final `v26.3`.

## 1. Resolve the RC boundary

Require the exact tag from the owner. Fetch the tag and matching patch branch,
then resolve both immutable SHAs and prove containment:

```bash
git fetch origin --tags stable-26-3-1
git rev-parse 26.3.1.16^{} origin/stable-26-3-1
git merge-base --is-ancestor 26.3.1.16 origin/stable-26-3-1
```

Stop if the tag is absent, does not belong to the expected `stable-X-Y-1`
branch, or if the owner has not identified the prior publicly announced release
line. A later patch-branch tip is not release evidence.

## 2. Inventory the exact RC tag

Use the exact tag as the target of every implementation and flag check. First
prove whether the previous release tag is an ancestor. When tags diverge,
inventory the target tag and its source/backport PRs instead of treating a
two-dot range as a chronological delta.

```bash
git merge-base <previous-tag> 26.3.1.16
git rev-list --left-right --count <previous-tag>...26.3.1.16
git rev-list --reverse --topo-order <previous-tag>..26.3.1.16
```

Do not use a moving stable branch, a symmetric three-dot log, or first-parent
history as the only raw inventory. Do not sample or drop merges before every
raw commit has an audit row.

Use patch equivalence only as a secondary deduplication signal:

```bash
git log --right-only --cherry-pick --no-merges \
  --format='%H%x09%s' \
  <previous-tag>...26.3.1.16
```

Keep a temporary evidence table outside the public docs diff with these fields:

| Commit | Integration PR | Source PR | YDBFEATURES | Net state | Decision | Reason |
|---|---|---|---|---|---|---|

Every raw commit must end with an explicit decision. Do not commit Tracker data
or the evidence table to the public repository. Assert that the number of unique
SHA values in this table equals `git rev-list --count` for the raw range. Read
commit metadata per SHA, or use NUL-delimited output; commit bodies are not safe
as tab/newline-delimited records.

## 3. Resolve PRs and feature tickets

For each commit:

1. Extract PR numbers from merge/squash subjects and trailers.
2. For `[Backport ...] PR #SOURCE ... (#BACKPORT)`, record both numbers. Use the
   source PR to understand the change and the backport PR to prove inclusion.
3. If the message has no PR, use GitHub commit-to-PR association.
4. Group multiple commits and backports belonging to the same source PR.
5. Read the source PR title, body, linked issues, changed files, surviving diff,
   and revert state.
6. Extract `YDBFEATURES-N` from commit and PR metadata.
7. For an explicit key, call Tracker MCP `GetIssue` first and request all fields.
   Read `type`, description, `Changelog entry`, documentation status, feature
   flag/configuration fields, and actual/planned code/default branches.
8. If no key is explicit, search Tracker MCP `GetIssues` in queue `YDBFEATURES`
   first by the repository-qualified PR URL, then by an exact linked
   implementation ticket. Verify the relationship in the result and call
   `GetIssue` for the candidate key. A PR number without repository context or a
   title-only semantic resemblance is not a confirmed match.

Match confidence is binary for publication: confirmed or unresolved. Unresolved
and unmatched rows remain research blockers because none of the supported
exclusion reasons has been proven. Never invent a Tracker match.

## 4. Build the authoritative Tracker candidate set

The code release keeps the first three input components for Tracker even though
the public RC heading uses only `X.Y RC`. For RC tag `26.3.1.16`, query
`stable-26-3-1` in Tracker.

1. Call Tracker MCP `GetQueueLocalFields` for queue `YDBFEATURES`.
2. Find the fields whose short keys are `actualCodeReadyBranch` and
   `actualDefaultInclusionBranch`. Use their full queue-local `id` values in
   structured `GetIssues` filters. Do not hardcode the opaque id prefix.
3. Run two independent queries. Each query filters by queue `YDBFEATURES`, type
   `feature`, and exact value `stable-26-3-1`, once for each factual field.
4. Request `key`, `type`, both factual release fields, documentation status,
   summary, and status. Read every page of both queries until
   `page == pages_count`.
5. Verify every returned ticket has type `feature`. Do not filter by workflow
   status or documentation status.
6. Build `expected_feature_keys` as the set union of the unique keys from both
   complete queries. A ticket present in both queries counts once.
7. Save the unmodified page payloads for both queries in a temporary Tracker
   export. Do not manually retype or select ticket keys for the eval.

The factual fields model two ways a feature enters a release: implementation
can become code-ready in that release, or an implementation delivered earlier
can become included by default in that release. Looking only at the first field
silently loses default-enabled features.

On 2026-09-08, the live `26.3.1` queries returned 27 code-ready tickets and 18
default-inclusion tickets with six overlapping keys, for a union of 39. This is
only a dated control observation: never hardcode it, and query the current
counts during every actual run. If the user provides a dashboard export, such
as an XLSX file with factual release rows, compare its unique ticket-key set
with the Tracker union before drafting notes. Treat the workbook as an
independent cross-check, not as a manually copied replacement for raw Tracker
pages.

## 5. Select only shipped features

For each key in `expected_feature_keys`, make exactly one of two decisions:
publish one bilingual release-note item, or record one exclusion. Publish only
after all evidence conditions are true:

- At least one surviving implementation change is reachable from the exact RC tag.
- The confirmed ticket key is in `expected_feature_keys`.
- PR code and ticket data show a new user-visible capability, not repair of
  existing behavior.
- The complete enabling implementation is present at the exact RC tag.
- The feature was not already documented in the previous release line.

For every candidate, independently inspect the feature-flags documentation and
the flag definition, default, runtime guard, effective overrides, and enabling
tests at the exact tag. Tracker branch fields and an implementation PR do not
prove default-on availability. Classify each published item as `publish-default`
or `publish-opt-in`; use `TBD` when administrator-controlled activation is not
a supported contract.

Exclude bug tickets, bug-only PRs that merely mention a feature ticket,
refactors, tests, CI/build work, dependencies, docs-only commits, reverts,
and feature-looking PRs outside `expected_feature_keys`. Classify every PR before
ticket-level grouping; an excluded bug PR cannot serve as feature evidence.

Group all surviving PRs for one ticket into one user-facing bullet. Never group
different Tracker tickets into one bullet, because coverage is one ticket to one
note. Record every omitted expected ticket in the temporary manifest with one of
these reason codes and a concrete explanation:

| Reason code | Use when |
|---|---|
| `not-in-target` | The complete implementation or enabling backport is absent from the target branch. |
| `bug-only` | The surviving target change repairs existing behavior and adds no feature. |
| `already-released` | The capability shipped and was documented in an earlier release line. |

An excluded ticket does not block the release notes when the evidence supports
one of these reasons. It does block the eval if it is omitted silently, appears
both as a note and an exclusion, or has an empty or unsupported reason.

Before writing, rank every published feature by its user usefulness. This is a
model judgment grounded in the audited user impact, not a PR-title or code-size
heuristic. Use `high` for broadly useful, material capabilities in common user
workflows; `medium` for a clear benefit to a substantial but narrower audience;
and `low` for specialist, administrative, connector-specific, or incremental
capabilities. Record a concise evidence-based rationale for each choice. Within
each default-state group, publish `high`, then `medium`, then `low`; retain
default-enabled features before opt-in features. RU and EN must use the same
order. Do not rank by implementation complexity, author, PR date, or marketing
language.

## 6. Write RU and EN release notes

Use the ticket's `Changelog entry` and user impact as the wording seed. Use the
surviving PR diff to constrain what actually shipped, including limitations and
supported interfaces. Do not copy raw PR titles, expose exact feature-flag names
or values, or use a PR as a documentation fallback.

Add or update only the release-line section. If no branch-local pattern exists,
use:

```markdown
## Version 26.3 RC {#26-3-rc}

Release date: TBD.

### Functionality
```

```markdown
## Версия 26.3 RC {#26-3-rc}

Дата выхода: уточняется.

### Функциональность
```

Do not add a `26.3.1.16` subsection. Do not invent a date. Put
`publish-default` bullets under `Functionality` / `Функциональность`, in the
same usefulness order in both locales. Then use `Disabled functionality` /
`Отключенная функциональность` for `publish-opt-in` bullets. Put this one
introductory sentence immediately under the heading: `The following
functionality is not enabled by default.` / `Перечисленная ниже
функциональность не включена по умолчанию.` Never repeat a per-bullet
default-state sentence or claim a future default-on release unless the owner
supplies release-specific wording.

RU and EN must have identical feature sets, default-state groups, order,
technical tokens, and public link targets. Prefer verified public documentation
for the target version with `?version=v26.3`; place an anchor after the query.
If no versioned page exists, use only a semantically equivalent public `main`
page with `?version=main`. If neither exists, leave the functionality bullet
unlinked. When Tracker says `Документация не нужна`, do not add a documentation
link or report a gap. Report every `main` fallback and every missing-documentation
item at handoff in a table: problem, YDBFEATURES ticket, implementation PR.

Tracker is an internal research source. Do not publish Tracker links or ticket
descriptions. Link GitHub PRs only when the surrounding changelog style calls
for them; prefer public product documentation links for feature bullets.

## 7. Run the blocking accounting eval

Before opening the PR, create two temporary machine-readable files. The Tracker
export wraps the raw page payloads and records the live filter:

```json
{
  "queue": "YDBFEATURES",
  "issue_type": "feature",
  "value": "stable-26-3-1",
  "fields": {
    "actualCodeReadyBranch": {
      "field_id": "<resolved-code-ready-field-id>",
      "pages": ["<unmodified GetIssues pages>"]
    },
    "actualDefaultInclusionBranch": {
      "field_id": "<resolved-default-inclusion-field-id>",
      "pages": ["<unmodified GetIssues pages>"]
    }
  }
}
```

The manifest has one row per intended bilingual note and one row per excluded
candidate. It does not declare its own Tracker denominator:

```json
{
  "release_tag": "26.3.1.16",
  "release_line": "26.3 RC",
  "notes": [
    {"ticket_key": "YDBFEATURES-...", "availability": "default", "usefulness": "high", "usefulness_rationale": "Broadly improves a common workflow.", "en": "...", "ru": "..."}
  ],
  "exclusions": [
    {
      "ticket_key": "YDBFEATURES-...",
      "reason_code": "not-in-target",
      "reason": "Complete implementation is absent from the target branch."
    }
  ]
}
```

Run these assertions:

```text
tracker_count = count(union(unique(code_ready_keys), unique(default_inclusion_keys)))
note_count = count(notes)
exclusion_count = count(exclusions)

count(unique(notes.ticket_key)) == note_count
count(unique(exclusions.ticket_key)) == exclusion_count
set(notes.ticket_key) intersection set(exclusions.ticket_key) == empty
set(notes.ticket_key) union set(exclusions.ticket_key) == set(expected_feature_keys)
every note has non-empty EN and RU text
every note has usefulness high, medium, or low and a non-empty rationale
within each availability group, notes are ordered high, medium, low
every exclusion has an allowed reason_code and non-empty reason
```

`note_count` may be smaller than `tracker_count`; the difference must be exactly
the valid exclusions. Count equality across notes and Tracker is no longer the
gate because it would force incomplete, bug-only, or previously released work
into public notes. Exact set accounting is blocking. Report unaccounted keys,
extra keys, duplicate keys, overlap between notes and exclusions, invalid
reasons, and tickets lacking EN or RU text. Never open the PR while the eval
fails.

`note_count` counts bilingual note pairs, not the sum of bullets in both files.
For 26 Tracker candidates with three valid exclusions, the expected output is
23 EN bullets and the same 23 RU bullets, represented as 23 note rows plus three
exclusion rows.

Resolve the installed skill directory from the runtime, then run the
deterministic eval against the actual files. Do not assume a Cursor-specific
installation path:

```bash
python3 <skill-directory>/scripts/evaluate_release_coverage.py \
  --manifest <temporary-manifest.json> \
  --tracker-export <temporary-tracker-export.json> \
  --en ydb/docs/en/core/changelog-server.md \
  --ru ydb/docs/ru/core/changelog-server.md \
  --en-downloads ydb/docs/en/core/downloads/ydb-open-source-database.md \
  --ru-downloads ydb/docs/ru/core/downloads/ydb-open-source-database.md
```

The evaluator derives `expected_feature_keys` from the union of both complete
Tracker exports; it never trusts a denominator declared by the manifest. It
checks both live totals, filters, pagination, exact ticket accounting, links,
absence of patch and bug subsections, exact ordered top-level bullets in both
Markdown sections, and the Linux, Docker, Source Code, and RC-anchor targets in
both downloads files.
This is a structural coverage gate, not a semantic oracle: because public
Markdown does not expose internal ticket keys, the evidence-table review must
still verify that each manifest row accurately describes its named ticket and
surviving PRs.
Static behavior cases live in [evals/evals.json](evals/evals.json); evaluator
unit tests live in [tests/test_evaluate_release_coverage.py](tests/test_evaluate_release_coverage.py).

## 8. Validate and open the RC release-notes PR

Create an isolated working branch from current `main`. The implementation
evidence remains the exact RC tag, not the branch used for documentation.

Before committing, verify:

- every raw commit has an audit decision;
- unique audit SHA count equals the raw `rev-list --count`;
- the blocking accounting eval passes;
- every published bullet maps to a surviving delta commit, source PR, and its
  exact expected `YDBFEATURES` ticket;
- every omitted expected ticket has a supported exclusion reason backed by the
  evidence table;
- no bug-only, reverted, duplicate, previously shipped, or unresolved item is
  present;
- RU and EN are semantically aligned;
- headings and anchors contain `26.3 RC` and `26-3-rc`, never `26.3.1.16`;
- every `publish-opt-in` item is after default-enabled items and has no public
  flag name or activation value;
- within each availability group, feature bullets are ordered by the documented
  model usefulness judgment from high to low;
- every feature link is versioned documentation, an audited `main` fallback,
  or deliberately absent, never an implementation PR fallback;
- the diff contains exactly four release files: RU/EN changelogs and RU/EN
  downloads tables;
- Linux archive, Docker manifest, and source tag are independently verified;
- every downloads table has the exact RC version, date, artifact coordinate,
  and RC changelog anchor in both locales;
- documentation build and link/style checks pass.

Commit and push only the isolated branch. For mutations in `ydb-platform/ydb`,
use `YDB_GH_TOKEN` without printing or persisting it. Create the original PR
with explicit base `main`, then verify `baseRefName` through GitHub API/CLI.

Suggested public title:

```text
docs: add YDB 26.3 RC server release notes
```

The PR description records target/previous boundary SHAs, reviewed commit count,
included public PRs, exclusion policy, and validation results. Do not expose
internal Tracker content. In the user-facing completion status, report the live
Tracker union count, published note count, and every excluded ticket with its
reason.

## Common mistakes

| Mistake | Correction |
|---|---|
| Publishing `26.3.1.16` | Use it only as exact implementation tag; publish `26.3 RC`. |
| Opening against a stable branch | Create the original RC release-notes PR against `main`. |
| Comparing with a moving patch branch | Use the exact RC tag for implementation and default-state evidence. |
| Treating every PR as a note | Audit every commit, then group by confirmed feature ticket. |
| Inferring a feature from an “Add” title | Require Tracker type `feature` and shipped behavior. |
| Adding fixes from the delta | Exclude them even when they are user-visible. |
| Requiring docs-ready status | Candidate membership comes from the union of both factual release fields. |
| Querying only code-ready | Also query `actualDefaultInclusionBranch`; otherwise default-enabled features disappear. |
| Requiring notes to equal Tracker count | Require notes plus justified exclusions to partition the Tracker union. |
| Silently omitting an unshipped ticket | Record `not-in-target` and report it in the completion status. |
| Publishing internal evidence | Keep Tracker and audit details outside the docs diff. |
| Linking a functionality bullet to a source PR because versioned docs are absent | Use verified `main` docs, otherwise leave it unlinked and report the gap. |
| Treating a default-false flag as a reason to omit a feature | Put proven supported opt-in functionality after default-on bullets without exposing its flag. |
