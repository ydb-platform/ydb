---
name: generate-internal-changelog
description: >-
  Use when a user asks to prepare or make an internal YDB release, including
  requests such as "сделай внутренний релиз", "подготовь внутренний релиз",
  or "internal release", and for YDB Server feature release notes tied to
  YDBFEATURES and stable-X-Y branches.
---

# Generate internal YDB Server changelog

## Overview

Build feature-only release notes from the complete commit delta between two
adjacent release-line branches. The union of the two factual Tracker release
fields defines the candidate set; commits and PRs decide which candidates
actually shipped. The release branch is the publication target.

In this workflow, a release line is `X.Y`. An input such as `26.3.1` identifies
release line `26.3`. The final component is input context only.

## Non-negotiable invariants

For input `26.3.1`:

| Item | Required value |
|---|---|
| Release line | `26.3` |
| Target ref | `origin/stable-26-3` |
| Previous ref | `origin/stable-26-2` |
| PR base | `stable-26-3` |
| Docs selector | `?version=v26.3` |
| Heading and anchor | `Version 26.3 {#26-3}` |
| Tracker code-ready value | `stable-26-3-1` |
| Tracker default-inclusion value | `stable-26-3-1` |

Never write `26.3.1` in the changelog heading, anchor, docs selector, working
branch, commit title, or PR title. Never target `main` or `stable-26-3-1`.

This workflow changes only:

- `ydb/docs/en/core/changelog-server.md`
- `ydb/docs/ru/core/changelog-server.md`

Do not update downloads tables. Do not publish a Bug Fixes section.

## 1. Resolve adjacent release lines

Fetch remote refs, then list only exact release-line branches matching
`stable-[0-9]+-[0-9]+`. Exclude patch, enterprise, hotfix, and test branches.

The target is `stable-X-Y`, where `X.Y` is the first two numeric components of
the requested version. The previous branch is the greatest existing release
line smaller than `X.Y`, not a guessed `X.(Y-1)` value. Thus `27.1` may follow
`26.4`.

Record both boundary SHAs before analysis:

```bash
git fetch origin --prune
git rev-parse origin/stable-26-2
git rev-parse origin/stable-26-3
git rev-list --count origin/stable-26-2..origin/stable-26-3
```

Stop if either exact branch is missing or the predecessor is ambiguous.

## 2. Inventory every target-only commit

The authoritative raw inventory is the two-dot set difference: every commit
reachable from the target and not reachable from the previous branch.

```bash
git rev-list --reverse --topo-order \
  origin/stable-26-2..origin/stable-26-3
```

Do not use a symmetric three-dot log as the raw inventory: it also contains
commits unique to the previous branch. Do not sample, use only first-parent
history, or drop merges before every raw commit has an audit row.

Use patch equivalence only as a secondary deduplication signal:

```bash
git log --right-only --cherry-pick --no-merges \
  --format='%H%x09%s' \
  origin/stable-26-2...origin/stable-26-3
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

The code release keeps all three input components even though the docs release
line drops the last one. For input `26.3.1`, query `stable-26-3-1` in Tracker.

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

- At least one surviving implementation change is in the raw target delta.
- The confirmed ticket key is in `expected_feature_keys`.
- PR code and ticket data show a new user-visible capability, not repair of
  existing behavior.
- The complete enabling implementation is present in the target branch.
- The feature was not already documented in the previous release line.

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

## 6. Write RU and EN release notes

Use the ticket's `Changelog entry` and user impact as the wording seed. Use the
surviving PR diff to constrain what actually shipped, including feature flags,
configuration, limitations, and supported interfaces. Do not copy raw PR titles.

Add or update only the release-line section. If no branch-local pattern exists,
use:

```markdown
## Version 26.3 {#26-3}

Release date: TBD.

### Functionality
```

```markdown
## Версия 26.3 {#26-3}

Дата выхода: уточняется.

### Функциональность
```

Do not add a `26.3.1` subsection. Do not invent a date.

RU and EN must have identical feature sets, order, technical tokens, and public
link targets. When public documentation exists, use the target-branch page with
`?version=v26.3`; place an anchor after the query, for example
`topic.md?version=v26.3#autopartitioning`. If Tracker says documentation is not
ready or not required, the ticket still needs a note; use the public source PR
as the link and describe only behavior proven by its surviving diff.

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
  "release_input": "26.3.1",
  "release_line": "26.3",
  "notes": [
    {"ticket_key": "YDBFEATURES-...", "en": "...", "ru": "..."}
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
  --ru ydb/docs/ru/core/changelog-server.md
```

The evaluator derives `expected_feature_keys` from the union of both complete
Tracker exports; it never trusts a denominator declared by the manifest. It
checks both live totals, filters, pagination, exact ticket accounting, links,
absence of patch and bug subsections, and exact ordered top-level bullets in
both Markdown sections.
This is a structural coverage gate, not a semantic oracle: because public
Markdown does not expose internal ticket keys, the evidence-table review must
still verify that each manifest row accurately describes its named ticket and
surviving PRs.
Static behavior cases live in [evals/evals.json](evals/evals.json); evaluator
unit tests live in [tests/test_evaluate_release_coverage.py](tests/test_evaluate_release_coverage.py).

## 8. Validate and open the release-branch PR

Create an isolated worktree and working branch from the exact target ref, for
example `docs/changelog-server-26-3` from `origin/stable-26-3`.

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
- headings, anchors, and links contain `26.3`, never `26.3.1`;
- the diff, working branch, commit title, PR title, and PR body do not leak the
  final component `26.3.1` as the published docs version;
- the diff contains only the two changelog files;
- documentation build and link/style checks pass.

Commit and push only the isolated branch. For mutations in `ydb-platform/ydb`,
use `YDB_GH_TOKEN` without printing or persisting it. Create the PR with explicit
base `stable-26-3`, then verify `baseRefName` through GitHub API/CLI. Also verify
that the working branch was created from the recorded target SHA rather than
from `main`.

Suggested public title:

```text
docs: add 26.3 server feature release notes
```

The PR description records target/previous boundary SHAs, reviewed commit count,
included public PRs, exclusion policy, and validation results. Do not expose
internal Tracker content. In the user-facing completion status, report the live
Tracker union count, published note count, and every excluded ticket with its
reason.

## Common mistakes

| Mistake | Correction |
|---|---|
| Publishing `26.3.1` | Normalize once to `26.3` and use it everywhere. |
| Opening against `main` | Create from and target `stable-26-3`. |
| Comparing with a patch branch | Compare exact adjacent `stable-X-Y` lines. |
| Treating every PR as a note | Audit every commit, then group by confirmed feature ticket. |
| Inferring a feature from an “Add” title | Require Tracker type `feature` and shipped behavior. |
| Adding fixes from the delta | Exclude them even when they are user-visible. |
| Requiring docs-ready status | Candidate membership comes from the union of both factual release fields. |
| Querying only code-ready | Also query `actualDefaultInclusionBranch`; otherwise default-enabled features disappear. |
| Requiring notes to equal Tracker count | Require notes plus justified exclusions to partition the Tracker union. |
| Silently omitting an unshipped ticket | Record `not-in-target` and report it in the completion status. |
| Publishing internal evidence | Keep Tracker and audit details outside the docs diff. |
