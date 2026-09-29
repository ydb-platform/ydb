---
name: ydb-release-publication
description: Publish a YDB Server release after its release-notes PR, including stable docs, GitHub Release, default branch, and Club YDB draft.
---

# Publish a YDB Server release

Run publication as a dependency chain. Do not mutate external state until the
release-notes gate passes.

## Gate

The original release-notes PR must be merged into `main`, have green
`build-docs`, required review, exact RU/EN headings and anchors, and four
delivery files: both changelogs and both downloads tables. Verify exact Linux
archive, Docker manifest, source tag, and no unresolved placeholders. If any
condition fails, return `BLOCKED` and name the exact missing condition.

## Publication order

1. Create focused stable-docs backports only from the merged main PR.
2. Wait for the `docs_release` workflow for the exact stable branch SHA. Verify
   build, upload, release, HTTP 200, and the release anchor on the published
   changelog page.
3. Verify the exact Git tag and absence of an existing release. Create a public,
   non-draft GitHub Release with exactly:

   ```text
   Release notes: https://ydb.tech/docs/en/changelog-server?version=vX.Y#x-y-z
   ```

   Read it back and verify tag, state, URL, and body.
4. Create a separate `main` PR changing only `ydb/docs/default-branch.txt`.
   Do not merge it without owner approval.
5. Create a `MODERATORS` Club YDB draft, retaining the approved image and
   working binary and release-notes links. Ask for approved wording on internal
   rollout before creating or editing it. Never infer rollout from an old
   template or OSS publication. Always include:

   ```sql
   SELECT Version();
   ```

Read every external object back after mutation and hand off all PR, workflow,
page, release, and draft links with pending owner actions.
