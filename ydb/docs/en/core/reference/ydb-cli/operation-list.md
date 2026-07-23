# Getting a list of long-running operations

Use the `ydb operation list` subcommand to get a list of long-running operations of the specified type.

General format of the command:

```bash
{{ ydb-cli }} [global options...] operation list [options...] <kind>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `kind`: The type of operation. Possible values:
  * `buildindex`: The build index operations.
  * `export/s3`: The export to S3 operations.
  * `import/s3`: The import from S3 operations.

View a description of the command to get a list of long-running operations:

```bash
{{ ydb-cli }} operation list --help
```

## Parameters of the subcommand {#options}

Name | Default value | Description
---|---|---
`-s`, `--page-size` | `10` (it is set on the server side and may vary depending on the kind of operation) | Number of operations on one page. If the list of operations contains more strings than specified in the `--page-size` parameter, the result will be split into several pages. To get the next page, specify the `--page-token` parameter.
`-t`, `--page-token` | `0` | Page token.
`--format` | `pretty` | Acceptable values:<ul><li>`pretty`: A human-readable format.</li><li>`proto-json-base64`: Protobuf result in [JSON](https://en.wikipedia.org/wiki/JSON) format, binary strings are encoded in [Base64](https://en.wikipedia.org/wiki/Base64).</li></ul>

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Get a list of long-running build index operations for the `series` table:

```bash
{{ ydb-cli }} -p quickstart operation list \
  buildindex
```

Result:

```text
┌───────────────────────────────────────┬───────┬─────────┬───────┬──────────┬─────────────────────┬─────────────┐
│ id                                    │ ready │ status  │ state │ progress │ table               │ index       │
├───────────────────────────────────────┼───────┼─────────┼───────┼──────────┼─────────────────────┼─────────────┤
│ ydb://buildindex/7?id=281489389055514 │ true  │ SUCCESS │ Done  │ 100.00%  │ /my-database/series │ release_idx │
├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴╴╴┤
│ Create time: 2023-04-20T11:04:29Z                                                                              │
│ End time: 2023-04-20T11:04:29Z                                                                                 │
├───────────────────────────────────────┬───────┬─────────┬───────┬──────────┬─────────────────────┬─────────────┤
│ ydb://buildindex/7?id=281489389055513 │ true  │ SUCCESS │ Done  │ 100.00%  │ /my-database/series │ title_idx   │
├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴╴╴╴┤
│ Create time: 2023-04-20T10:58:29Z                                                                              │
│ End time: 2023-04-20T10:59:29Z                                                                                 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

Next page token: 0
```

By default, `buildindex` displays only the first 10 operations, sorted by `id` in descending order. New operations are typically assigned a larger identifier, but this is not guaranteed. If a new operation is assigned an `id` smaller than existing ones, it may appear on a different page.