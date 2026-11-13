# Deliver the results

## Tests only

1. Commit only the test files and the `ya.make` lines they need: the list of changed files must have nothing outside `ydb/core/kqp/ut/`. No fix, also not in a second commit of the same PR: the commit authors fix the code. When the user wants a fix anyway, it goes into a separate PR.
2. Leave out tests that pass on the base (stress tests that found nothing, tests of rejected candidates).
3. Follow the commit and PR conventions the user gave you (trailers, title prefixes, PR body length).

## Report

Write the report in `<worktree>-out/report.md`. Sections:

1. Base commit and the list of audited commits.
2. Reproduced: for each bug the commit, mechanism with file:line, the SQL trigger and the knobs it needs, one line of the failure, the result with the commit reverted, the test name.
3. Real or uncertain, not reproduced: each test design tried, and for stress runs the duration, iterations and outcomes.
4. Rejected after verification, with the reason.
5. Out of scope.
6. Existing reports found by the search below.

Share the report the way the user asks (for example a paste service) and link it from the PR.

## Search for existing reports

Search the GitHub issues of `ydb-platform/ydb`, open and closed, for the failure text and for the function or file name, for example with `gh issue list -R ydb-platform/ydb --search "<text>" --state all`.

Ask the user which tracker queues to search, and search them with the same words, the commit sha and the PR number. When a search cannot run (no access, no network), write that in the report.

## Draft PR

Publish the branch to your fork and open a draft PR against `main` of `ydb-platform/ydb` with the tool you use, for example `gh pr create --draft`. The body has the `### Changelog entry` section with `* Not for changelog: test only.` and one line for reviewers: which bugs the tests show, which commits caused them, and the report link.

## Ticket

1. One ticket per period. Mention only the bugs of this period; do not put earlier periods into it.
2. Assign it to the author of the commit that introduced the bug, or ask the user.
3. Link only issues about the same defect, or the task or PR that introduced it when that relation is documented (the PR names the ticket). Do not link issues that match only by assert text, by theme or by component, and do not link issues about candidates that were not reproduced. Put such findings in the report instead.
