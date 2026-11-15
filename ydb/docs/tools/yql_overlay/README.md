# YDB additions to common YQL documentation

`prepare.py` builds a disposable copy of `ydb/docs` using the common YQL sources
and YDB additions from the **same checkout**. It does not edit either source tree.
The existing documentation build and release workflows use this copy only when
`ydb/docs/tools/yql_overlay/enabled` exists in the checked-out revision.
This change deliberately does not add that activation file.

## File ownership

`manifest.json` records the baseline commit and the source-to-destination mapping
for both locales. Paths are relative to `yql/essentials/docs/<locale>` and
`ydb/docs/<locale>/core/yql/reference`, respectively.

- `import`: copy the common page unchanged.
- `include`: copy the common page containing optional `YDB` branches
  and install the corresponding files from `overlays/<locale>` beside it.
  The preparer enables the variable in the **output** presets. Ordinary YQL builds
  leave it unset and use the original text without requiring the YDB fragments.
- `keep`: retain the complete YDB page. The reason is recorded per page. An upstream
  hash change stops preparation so the override cannot silently hide a new update.

All other YDB pages, includes, navigation, images and configuration stay in the
copied tree. New upstream paths are not automatically published: extend the manifest
and review their applicability and navigation first.

The initial migration preserves the existing output, including local editorial
changes and previously unimported upstream changes. These fragments are therefore
not all permanent product differences. Content reconciliation is a separate change.

The target hash on imported pages prevents overwriting edits made directly to the
old YDB copy after extraction. Edit the corresponding fragment instead, or explicitly
reconcile and revalidate the page before updating the hash. Missing or changed hook
inventories and missing fragments fail before any output is written.

## Rollout

1. Merge the Arcadia YQL extension points and wait for their normal export to the
   GitHub branch. Do not copy an entire newer rightlib into stable for this purpose.
2. Merge this manifest, fragments and preparer. Existing builds remain unchanged.
3. Verify the composed build against the pinned baseline. Merge the separate CI and
   activation change last. Stable branches opt in independently after compatible
   hooks and additions have been backported through `cherry_pick_v2.yml`.

The common workflows are referenced at `@main`, but activation and all content
belong to the checkout being built. A missing activation file retains the old build;
an enabled but incomplete installation is an error.

## Local verification

Use a disposable checkout containing both PRs' inputs. Without merging activation:

```bash
python3 ydb/docs/tools/yql_overlay/prepare.py \
  --output /tmp/ydb-docs-overlay-preview --preview
```

The output directory must not exist and must be outside the checkout. Build the
baseline and prepared trees with the same Diplodoc version, configuration and
presets. Compare rendered content, headings, links, navigation and warnings for
RU and EN. Normalize only demonstrated generated random HTML IDs. Also build the
common YQL source without YDB files and compare its output before and after hooks.

Tests and comparison artifacts are maintained locally, outside these PRs.
