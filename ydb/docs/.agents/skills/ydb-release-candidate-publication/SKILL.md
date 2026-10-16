---
name: ydb-release-candidate-publication
description: "Use when publishing a YDB Server release candidate from ydb/docs as a GitHub pre-release; use ydb-release-publication for final releases."
---

# Publish a YDB Server release candidate

Publish an already documented YDB Server release candidate as a GitHub
pre-release. For a final release, use `ydb-release-publication` instead.

## Inputs

Collect the exact server tag, release line `X.Y`, release-notes PR, stable
documentation branch, owner-approved release date, and any focused remediation
PR that supersedes a failed delivery check. Resolve every ref to an immutable
SHA before checking it.

## Gate

Use `check-release-notes` when it is available. Before any external mutation,
verify all of these conditions:

1. The original release-notes PR is merged into `main`, and its focused
   backport is merged into `stable-X-Y`.
2. Both locales use `Version X.Y RC` / `Версия X.Y RC` and anchor `x-y-rc`.
3. The RU and EN changelogs contain the approved date and no `TBD`,
   `уточняется`, or unresolved artifact placeholder.
4. The four release delivery files contain the exact version and consistent
   dates: RU/EN `changelog-server.md` and RU/EN
   `downloads/ydb-open-source-database.md`.
5. Required review and CI are green on the exact delivery PR SHAs. A failed
   delivery check may be superseded only by a later focused remediation PR with
   green required checks, merged into the same target branch, and visible on
   the published pages. Record both immutable SHAs and the supersession.
6. The Linux archive returns HTTP 200, `docker manifest inspect` succeeds for
   the exact image tag, and the exact Git tag exists.
7. The published RU and EN `?version=vX.Y` pages return HTTP 200 and contain
   the RC heading, anchor, and approved date.
8. A GitHub Release does not already exist for the tag.

If a condition fails, return `BLOCKED` with the exact file, check, URL, or
artifact and the action that removes the blocker. A focused correction PR is
allowed when the owner asked for a fix, but do not create the GitHub Release
until the correction is merged and visible on the published pages.

## RC-only publication

An RC publication stops after the GitHub pre-release. Do not run or dispatch
`docs_release`, change `ydb/docs/default-branch.txt`, or create a Club YDB
draft or post. Those steps belong only to `ydb-release-publication` for a final
release.

Build the body from the verified public anchor. It must be exactly:

```text
Release notes: https://ydb.tech/docs/en/changelog-server?version=vX.Y#x-y-rc
```

Create the release with the project token and explicit RC flags:

```bash
env -u GITHUB_TOKEN GH_TOKEN="$YDB_GH_TOKEN" gh release create "$version" \
  --repo ydb-platform/ydb \
  --verify-tag \
  --title "$version RC" \
  --notes "$body" \
  --prerelease \
  --latest=false
```

Do not generate a missing tag. Do not use `--generate-notes`, a bare URL, an
RU URL, or a final-release anchor.

## Verification and handoff

Read the release back with `gh release view`. Confirm the exact tag, title
`<version> RC`, body, URL, `isDraft=false`, and `isPrerelease=true`. Call
`GET /repos/ydb-platform/ydb/releases/latest` and confirm its `tag_name` is not
the RC tag. Recheck the public EN release-notes URL.

Report the release-notes PR, stable backport PR, immutable tag target,
artifacts, published RU/EN pages, and GitHub pre-release URL. State explicitly
that no documentation workflow, default-branch PR, or Club YDB draft was
created.
