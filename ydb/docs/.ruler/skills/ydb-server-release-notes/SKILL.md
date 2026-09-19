---
name: ydb-server-release-notes
description: Prepare or audit public YDB Server release notes, including release candidates, downloads, documentation links, and stable backports.
---

# Prepare YDB Server release notes

Use this skill for public server release notes. Use
`generate-internal-changelog` instead for feature-only internal RC notes.

## Before drafting

1. Resolve the exact target tag and immutable SHA, release line, prior public
   release line, and target documentation branches. A moving stable branch is
   not a substitute for a tag.
2. Run the release-readiness audit. For every YDBFEATURES candidate, record
   implementation at the exact target, public surface and limitations, Tracker
   state, feature-flag definition/default/runtime guard, source and backport
   PRs, previous RU/EN announcement, documentation, glossary terms used in
   public prose, and disposition.
3. Check default state in target-ref code and feature-flag documentation. A
   Tracker branch field or an old source PR does not prove default-on status.
4. Verify Linux archive, Docker manifest, source tag, all public links, and
   RU/EN structure separately.

## Public content policy

- Publish a proven user-facing capability that administrators can enable as a
  supported contract even if it defaults to false.
- Put default-on bullets first under `Functionality` / `Функциональность`.
  Put supported opt-in bullets after them under `Disabled functionality` /
  `Отключенная функциональность`. Do not expose flag names, configuration keys,
  or activation values in public prose.
- State material scope restrictions, including object and table types. Split a
  bullet when default state, public surface, scope, or release transition differ.
- Publish a feature only at first public availability, or when its public state
  changes. Code in an older branch is not proof of an earlier announcement.
- Include a functionality bullet only when a public documentation page supports
  its exact claim. Prefer `?version=vX.Y`; if unavailable, use a verified
  equivalent `?version=main` page. If neither exists, omit the bullet from the
  public release notes and report the documentation gap separately. Never use a
  PR or issue as a documentation fallback.
- A Tracker value of `Документация не нужна` does not bypass the documentation
  gate. If no public page supports the exact claim, omit the item from public
  release notes. Do not use that Tracker value to justify an unlinked bullet.
- Before drafting, read `ydb/docs/ru/core/concepts/glossary.md` and
  `ydb/docs/en/core/concepts/glossary.md`. For every YDB-specific term in
  release-note prose, use only the canonical wording from the corresponding
  glossary. Do not invent translations, variants, or implementation names.
  The glossary controls terminology but does not replace the required feature
  documentation page.
- Link the feature name or public operation to the narrowest matching
  documentation section. A general feature-flags page is not evidence for a
  user-facing functional claim when dedicated documentation exists.
- End the handoff with a table of every `main` fallback or missing-documentation
  item: problem, YDBFEATURES ticket, implementation PR. Write `None` if empty.

## RC and final release shape

An RC uses `Version X.Y RC` / `Версия X.Y RC` and anchor `x-y-rc`. Do not show
its patch tag as the documentation version. Every server release-notes PR,
including an internal RC, changes four files: RU and EN changelogs plus RU and
EN downloads tables.
For an RC, the downloads version group is `vX.Y RC`, not final `vX.Y`.
Within default-enabled and disabled groups separately, order features by model
judgment of user usefulness from high to low, and keep that order identical in
RU and EN.

Open the original PR against `main`. Only after it merges, create focused
backports containing no later cleanup or translation changes. Verify CI and
the exact resulting head SHA before declaring the PR ready.
