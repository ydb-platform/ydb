# Selective YQL documentation synchronization

This tool updates only YQL reference files that YDB has not changed. It runs through the selective mode of `yql/essentials/docs/scripts/doc_upgrade_git.sh`. The existing script mode remains available to other consumers.

## Ownership

`ownership.json` contains an explicit entry for each mapped source or destination path in the RU and EN reference trees. An entry with `owner: ydb` is never synchronized. Ownership only moves from `yql` to `ydb`; the tool never moves it back.

Initialization compares each current destination with the source at the revision recorded in that locale's `essentials.base_rev`. A Markdown file with identical contents and mode can remain under YQL ownership. Different, missing, renamed, or YDB-only files are excluded. Navigation files and assets stay under YDB control. New upstream files are reported but never imported automatically. This gives a fixed, shrinking set of synchronized pages.

On subsequent runs the tool checks destination content and the full history since its checkpoint, including merged branches. Any local change, including a committed edit followed by a committed revert, transfers the file to YDB ownership. A sync commit is recognized by its ownership-state update and matching source and destination Git blobs, not by the author or commit message. Changes squashed away before reaching the main branch are not visible to this history check.

Deleting or renaming a YDB page prevents its recreation. If an upstream page disappears or moves, the destination is preserved and transferred to YDB ownership for manual review. No target files are automatically deleted. New destination files are automatically registered as owned by YDB.

## Initial inventory

The initial comparison leaves 19 RU and 16 EN Markdown files eligible for synchronization. All other existing reference files and source-only paths from the baseline are registered as YDB-owned. Exact paths and reasons are in `ownership.json`; the generated per-run report lists proposed updates and ownership transitions.

## Local use

Use a clean checkout with full history. Dirty worktrees, shallow clones, missing baseline commits, unsafe paths, and symlinks are rejected before writes.

From the repository root, inspect a proposed update without changing files:

```bash
bash yql/essentials/docs/scripts/doc_upgrade_git.sh --selective --report /tmp/yql-docs-sync.json
```

To apply that comparison in your checkout:

```bash
bash yql/essentials/docs/scripts/doc_upgrade_git.sh --selective --apply --report /tmp/yql-docs-sync.json
```

Review the changed pages and `ownership.json` together. Commit both in the same commit so that the next run can distinguish a synchronization from a local edit. Build the documentation before merging. A second run after committing an update produces no changes unless the source or destination changed again.

The initial configuration is generated once with `--selective --initialize`. Initialization refuses to replace an existing configuration. Do not regenerate it to resolve a synchronization error: that would discard ownership history. The old `essentials.base_rev` files remain historical inputs and are not advanced by selective synchronization; per-file source revisions are stored in the ownership configuration.

## Daily workflow

`.github/workflows/yql_docs_sync.yaml` runs at 02:00 UTC, which is 05:00 Moscow time. GitHub may delay scheduled runs. It reads source and destination from the same `main` checkout and creates or updates one `automation/yql-docs-sync` PR. It never merges that PR automatically. A manual run defaults to dry-run mode. No PR is created when neither content nor ownership changed.

The workflow uses the existing `YDBOT_TOKEN` secret so that its PR triggers documentation checks. If the open sync branch contains commits with a committer other than the workflow bot, the next run stops instead of overwriting reviewer edits. Merge or close that PR and delete its branch before resuming automated updates. This guard protects ordinary reviewer commits; it is not an authentication mechanism.

It becomes scheduled after this workflow reaches the default branch. The selective shell entry point is preserved by the `rightlib_sync.yml` overwrite exclusion; its Python implementation and ownership state live outside the exported YQL subtree.

Run the integration tests with:

```bash
python3 -m unittest discover -s ydb/docs/tools/yql_sync -v
```
