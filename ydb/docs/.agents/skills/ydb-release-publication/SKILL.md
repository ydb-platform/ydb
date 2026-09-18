---
name: ydb-release-publication
description: Use when publishing a YDB Server release after its release-notes PR, including stable documentation, GitHub Release, default documentation branch, or Club YDB announcement.
---

# Publish a YDB Server release

Run the release as a dependency chain. A release-notes PR is a gate, not a
drafting aid. Do not start any publication while a gate is unproven.

## Inputs

Collect the exact server version, release-notes PR, source tag, required stable
documentation branch or branches, and approved Club YDB announcement template.
Resolve every ref to an immutable SHA before checking its content.

## Release-notes PR gate

Before any external mutation, verify that the original PR is merged into `main`
and has exactly these delivery files, unless the owner has approved a specific
exception:

| Locale | Release notes | Downloads |
| --- | --- | --- |
| RU | `ydb/docs/ru/core/changelog-server.md` | `ydb/docs/ru/core/downloads/ydb-open-source-database.md` |
| EN | `ydb/docs/en/core/changelog-server.md` | `ydb/docs/en/core/downloads/ydb-open-source-database.md` |

Use `check-release-notes` for the complete audit when it is available. At a
minimum, require green `build-docs`, required review, release heading and
anchor in both locales, verified binary archive, Docker manifest, source tag,
and no `TBD` or unresolved artifact placeholders. The RU and EN downloads rows
must contain Linux binary, Docker image, and source links for the exact version.

If any condition fails, return `BLOCKED` with the exact file, check, artifact,
or review that must be fixed. Do not create a backport, publish documentation,
create a GitHub Release, change the default branch, or draft an announcement.

## Stable documentation

After the gate passes, create focused release-notes backport PRs only from the
merged `main` PR, one per agreed stable branch. Do not add later cleanups or
translations. After merge, find the `docs_release` workflow triggered for that
branch and SHA. Do not manually dispatch a duplicate: the release workflow has
a branch concurrency group.

Proceed only after its build, upload, and release jobs succeed. Verify the
published `https://ydb.tech/docs/en/changelog-server?version=vX.Y` page returns
HTTP 200 and contains the exact release anchor.

## GitHub Release

First verify that the exact Git tag exists and a GitHub Release does not already
exist. Create a public, non-draft, non-prerelease GitHub Release named by the
exact server version. Its body is exactly:

```text
Release notes: https://ydb.tech/docs/en/changelog-server?version=vX.Y#x-y-z
```

Read the release back after mutation. Confirm its tag, public state, URL, and
body. Do not use a bare URL or an RU changelog URL in this body.

## Default documentation branch

Create a separate PR against `main` that changes only
`ydb/docs/default-branch.txt` from the previous stable branch to the newly
published one. Verify its remote SHA, one-file diff, and CI. Do not merge it
unless the owner explicitly asks.

## Club YDB announcement

Create a `MODERATORS` draft in Club YDB before any external announcement.
Preserve the approved template image. Include working binary and versioned
release-notes links. Verify the draft's club, visibility, title, text, image,
and links after creation. Do not publish beyond Club YDB without explicit
approval.

Before writing or editing the draft, ask the owner for approved wording about
internal availability or installation. Do not claim the release is installed
on all internal, production, or preproduction clusters based on an old template
or the OSS release status. When the owner has already supplied the wording,
use it without asking again.

Every Club YDB release draft must also explain how to identify the database
version. Include this text and query without asking the owner whether to add
it:

```text
Версию базы данных можно узнать, выполнив запрос:
```

```sql
SELECT Version();
```

## Handoff

Report the release-notes PR, every backport PR, `docs_release` run, published
page, GitHub Release, default-branch PR, and Club YDB draft. State any pending
review or owner action explicitly.
