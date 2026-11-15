# YDB additions to common YQL documentation

`prepare.py` composes a disposable copy of `ydb/docs` from the common YQL sources
and YDB additions in the **same checkout**. Existing build and release workflows
use this copy when `ydb/docs/tools/yql_overlay/enabled` exists in that revision.
This PR does not add the activation file.

The migration deliberately accepts common wording and common library updates.
It does not preserve every historical textual difference. See
[the reconciliation inventory](RECONCILIATION.md) for accepted changes and unresolved
pages that remain local. This is a partial migration, not a claim that all YQL
pages are now synchronized.

## Ownership

`manifest.json` maps paths relative to `yql/essentials/docs/<locale>` to paths
relative to `ydb/docs/<locale>/core/yql/reference`:

- `import`: use the common article.
- `include`: use the common article plus additive `if YDB / include / endif`
  extension points. There are no alternative common/YDB paragraphs and no `else`.
  Install files from `overlays/<locale>` and enable `YDB` in the output presets.
- `keep`: retain the complete existing YDB article. Each entry explains why.
  An upstream hash change fails preparation and requires reconciliation. These
  entries include unresolved common corrections, not only permanent YDB differences.

All other YDB articles, images, navigation and configuration remain in the copied
tree. New common paths require an explicit mapping and navigation review.

Relative links follow the source-to-target mapping. Explicit aliases handle
consumer anchors and links to common release notes at the reviewed source revision.
The latter describe YQL language versions, not YDB server versions. New release-note
references must be reviewed and added to the aliases. Legacy incoming anchors are
preserved at explicit markers; marker drift is an error. Heading-level gaps and
missing spacing before headings are normalized in the output, outside code fences.
These layout adaptations do not require conditional paragraphs in the common docs.

Target hashes prevent overwriting direct edits to imported YDB copies after the
reviewed baseline. Missing fragments, replacement or unguarded hooks, and changed
hook inventories fail before writing output. Edit a YDB addition in its fragment;
reconcile direct edits before refreshing a target hash. Updating a `keep` source
hash is an explicit review decision, not an automatic way to silence CI.

## Rollout

1. Merge the Arcadia hooks and wait for normal export to GitHub. Confirm the mapped
   sources contain exactly the additive hooks listed in the manifest.
2. Merge the manifest, fragments and preparer. Existing builds remain unchanged.
3. Build baseline and composed documentation with identical Diplodoc configuration.
   Review the expected changes and remaining exclusions in the reconciliation report.
4. Merge the separate CI/activation PR last. An enabled but incomplete installation
   fails; a revision without the marker keeps the existing build path.

The common workflows are referenced at `@main`, while activation and content belong
to the checked-out revision. Stable branches opt in independently through
`cherry_pick_v2.yml`, after compatible hooks and additions are present. Do not import
an entire newer rightlib just to obtain documentation hooks.

## Local verification

In a disposable checkout containing both PRs' inputs:

```bash
python3 ydb/docs/tools/yql_overlay/prepare.py \
  --output /tmp/ydb-docs-overlay-preview --preview
```

The output directory must not exist and must be outside the checkout. Compare HTML,
headings, links, incoming anchors, navigation and build warnings for RU and EN.
Identical old YDB output is no longer the acceptance criterion: intentional common
updates are listed in the report. Build common YQL without YDB files as well; its
rendered output must remain unchanged. Tests and build artifacts stay outside PRs.
