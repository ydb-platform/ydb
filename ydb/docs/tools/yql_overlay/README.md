# YQL documentation overlay

The existing documentation CI builds the RU and EN `WITH` pages from the
imported `yql/essentials/docs` source and separately maintained YDB additions.
This is the first page migrated to composition; other pages keep their current
sources.

## Build sequence

Both `docs_build_core.yaml` (PR previews and rebuilds) and
`docs_release_core.yaml` (publication) perform the same preparation:

1. Check out the revision being built.
2. Generate the version-local feature flag defaults when enabled. PR builds also
   disable their PDF output as before.
3. Prepare a documentation tree in a fresh runner temporary directory. It
   contains the updated presets and configuration from the preceding steps.
4. Pass that directory as `src-root` to the existing Diplodoc build action.
   Both its HTML validation and Markdown publication build use the composed
   input. Existing upload and release jobs consume the resulting artifacts.

No new workflow or synchronization job is needed. Changes under
`yql/essentials/docs/**` trigger the existing PR and release workflows and are
recognized by their change filters and the `rebuild_docs` handler. Piglet and
rightlib continue delivering the common source without modification.

Older PR revisions and stable branches that do not contain `prepare.py` use the
original documentation root. A revision that has the preparer but is missing
its common source or wrappers fails preparation; it does not silently publish
stale content. No stable branch is changed by this PR.

The reusable workflows are called from `main`, so these CI steps become active
when the change lands in `main`. A pre-merge run using the current `main`
workflow does not exercise the new step.

## Composition

`prepare.py` copies `ydb/docs` into the temporary tree, replaces only the two
`with.md` pages with wrappers from `overlays`, and places the imported source
next to each wrapper as `_yql_with.md`. YFM includes compose the final pages.
Keeping the source fragment at the original page depth preserves its relative
links. The known upstream `EACH` links are adapted to `index.md`, where YDB
publishes the description. The checkout and imported files remain unchanged.

YDB file-source parameters, watermark descriptions and examples live in the
wrappers. Existing additional hints, including YT-related hints, are retained
without reassessing their applicability. The complete shared page appears
first, including its examples, followed by the additions. In particular, the
upstream empty-table paragraph is unconditional, unlike the former YDB
`feature_bulk_tables` block. Review these content differences when reviewing
this migration.

## Scope

Tests and comparison tools are kept locally for this change. Validation covers
preparation, actual CI step execution, source-root routing, compatibility with
older checkouts, upstream-only change detection and rendered content.

The standalone local `build.sh` and Arcadia consumer are not changed here.
Before migrating more pages, inventory their intentional YDB differences and
consider stable shared include fragments for additions between common sections.
Whole-file replacements must not hide incoming changes to the shared text.
