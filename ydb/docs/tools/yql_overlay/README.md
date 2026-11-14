# YQL documentation overlay pilot

This opt-in pilot composes the RU and EN `WITH` pages from the imported
`yql/essentials/docs` source and separately maintained YDB additions. It does
not change production publication, the existing pages, or the Arcadia import.

## Prepare and preview

Run from the repository root, with Python 3.10 or newer:

```bash
python3 ydb/docs/tools/yql_overlay/prepare.py --output /tmp/ydb-yql-docs
```

The output must be a new directory. Preparation retains the production `.yfm`,
including its PDF and single-page settings. For a content-only HTML preview,
replace the configuration in the generated directory and build it:

```bash
printf 'langs: [ru, en]\nallowHTML: true\napply-presets: true\n' > /tmp/ydb-yql-docs/.yfm
yfm -i /tmp/ydb-yql-docs -o /tmp/ydb-yql-html --strict --apply-presets --allowHTML
python3 -m http.server --directory /tmp/ydb-yql-html
```

Open `/ru/yql/reference/syntax/select/with.html` or the corresponding EN page.
On macOS use `/private/tmp` instead of `/tmp` in the commands passed to Diplodoc
because its scope validation distinguishes symlink paths.

Preparation copies the documentation tree, replaces only the two opted-in
`with.md` pages with the wrappers from `overlays`, and places the imported
source next to each wrapper as `_yql_with.md`. A normal YFM include composes the
page. Keeping the source fragment at the original page depth preserves its
relative links. The known upstream `EACH` links are adapted to `index.md`,
where YDB publishes the description; the imported files are never modified.

YDB-specific file-source parameters, watermark includes and examples live in
the wrappers. Existing additional hints are retained without reassessing their
product applicability. The complete shared page appears first, including its
examples, followed by the additions. The former duplicated common paragraphs
and examples now come from upstream. In particular, the upstream empty-table
paragraph is unconditional, unlike the previous YDB `feature_bulk_tables`
block. This semantic difference needs product review before rollout.

## Build integration

No new CI workflow is introduced by this pilot. Preparation is intended to run
in the existing documentation build, after checkout and before Diplodoc. The
builder then consumes the staging directory instead of `ydb/docs`. Both inputs
must come from the same checked-out revision: `yql/essentials/docs` and the YDB
wrappers. Piglet and the rightlib import continue to deliver the common source.

The pilot leaves the production build entry points unchanged. It can be run
manually for review using the commands above. Tests and comparison tools are
maintained locally for this experiment and are not part of this change.

## Before production rollout

Review the two text diffs and page layout, confirm the changed common wording
and feature applicability, then decide how to split larger shared pages into
stable include fragments for insertion between shared sections. A whole-file
overlay must not accidentally hide incoming common changes.

Inventory other pages and their intentional YDB differences before opting them
in. Integrate preparation into both local and CI build/publication entry points,
including the Arcadia consumer, and ensure upstream-only imports trigger those
builds. Keep stable branches using their own imported sources. The current pilot
deliberately leaves all of those production paths unchanged.
