# Documentation PR labels in Reactor

[Reaction](https://reactor.yandex-team.ru/browse?selected=116513772)
([queue](https://reactor.yandex-team.ru/browse?selected=116513649)),
[Nirvana graph](https://nirvana.yandex-team.ru/flow/64171971-f396-48e6-9c11-c663523f4b14/81f0d206-6567-49eb-beaf-dcb1c212f314/graph).

This replaces `.github/workflows/docs_label.yaml`. Every five minutes
(`0 0/5 * * * ?`), the reaction reconciles the `documentation` label on all
open pull requests in `ydb-platform/ydb`, including drafts and forks:

- Add the label if any current changed filename starts with `ydb/docs/`.
- Remove the label if no changed filename has that prefix.
- If 3000 files have been read without finding documentation, preserve the
  current label because the file list may be capped, matching the old workflow.
- Preserve every other label. A renamed file is classified by its current path.

A full scan also catches manually changed labels and catches up after downtime.
Closed PRs are not reconciled. Polling replaces event delivery, so changes are
normally reflected on the next five-minute run rather than immediately.

The program batches GraphQL reads and follows pagination. Up to eight independent
file-list requests run concurrently. All scanning finishes before any writes.
Before each mutation it verifies that the PR remains open, that its head and
base revisions have not changed, and that the label still needs changing.
Ambiguous results are skipped and reported. GraphQL errors fail the run instead
of interpreting partial data as an empty file list. The next run reconciles any
changes that race with the final API write; GitHub label writes have no revision
compare-and-swap.

The graph executes a reviewed copy of `sync.py` in a Python Run block. It does
not download, check out, or execute PR code. It needs only Python's standard
library. The `github_token` option is a Nirvana `Secret` with permission to read
PR files and add/remove issue labels in this repository. Never put a token value
in the code, configuration files, logs, or comments.

`apply_labels=false` produces a report without writes. After validating a
read-only run, set it to `true`, validate a real run, finalize that workflow
instance, and activate the reaction. **Do not merge the workflow removal before
this cutover is complete.** The repository workflow remains active until then.

The reaction uses quota `kikimr` and user `iuriisintiaev`. Its queue allows one
running and one waiting launch; missed ticks are coalesced (`FIRE_ONE`).
The block has a ten-minute execution timeout, 512 MB RAM, network
`KIKIMRDEVNETS`, `job-use-dns64=true` for GitHub connectivity, disabled cache,
and `force_restart` result reuse policy.

## Local checks

With `GH_TOKEN` supplied by the environment, the default is read-only:

```sh
python3 ydb/ci/docs_label/sync.py
```

Use `--apply` to update labels. JSON output lists proposed/applied changes,
skipped PRs with reasons, and the number of API requests. Progress goes to stderr.

```sh
python3 -m unittest discover -s ydb/ci/docs_label -v
```

## Updating the deployed graph

Update `sync.py` and its tests in a PR. Copy the reviewed source into the Python
Run `code` option, omitting the `if __name__ == '__main__': main()` entry point,
and append this adapter:

```python
report = reconcile(GitHub(github_token), apply=apply_labels)
out.write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
```

Record the source commit and validation result in the graph comment. Test with
`apply_labels=false` before enabling writes. The reaction follows the latest
finalized instance of its dedicated workflow; editing an unfinalized draft does
not constitute deployment. To pause the service, deactivate the Reactor reaction.
