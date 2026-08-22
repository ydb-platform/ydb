# Getting a list of background operations

Using the `ydb operation list` subcommand, you can get a list of background operations of the specified type.

General command format:


```bash
{{ ydb-cli }} [global options...] operation list [options...] <kind>
```


* `global options` — [global parameters](commands/global-options.md).
* `options` — [subcommand parameters](#options).
* `kind` — operation type. Possible values:

  * `analyze` — [ANALYZE](../../yql/reference/syntax/analyze.md) statistics collection operations
  * `buildindex` — index building operations
  * `compaction` — table compaction operations
  * `export/s3` — export to S3 operations
  * `export/nfs` — export to NFS operations
  * `import/s3` — import from S3 operations
  * `import/nfs` — import from NFS operations
  * `scriptexec` — script execution operations
  * `incbackup` — incremental backup operations
  * `restore` — restore from backup operations
  * `setnotnull` — operations for setting the `NOT NULL` limit.

View the description of the command for getting a list of background operations:


```bash
{{ ydb-cli }} operation list --help
```


## Subcommand parameters {#options}

| Name | Description |
| --- | --- |
| `-s`, `--page-size` | Number of operations per page. If the operation list contains more rows than specified in the `--page-size` parameter, the output will be split into multiple pages. To get the next page, specify the `--page-token` parameter. |
| `-t`, `--page-token` | Page token. |
| `--format` | Output format.<br/>Default value — `pretty`.<br/>Possible values:<ul><li>`pretty` — human-readable format</li><li>`proto-json-base64` — Protobuf output in [JSON](https://en.wikipedia.org/wiki/JSON) format, binary strings are encoded in [Base64](https://en.wikipedia.org/wiki/Base64).</li></ul> |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Get a list of background index building operations for the `series` table:


```bash
{{ ydb-cli }} -p quickstart operation list \
  buildindex
```


Result:


```text
┌───────────────────────────────────────┬───────┬─────────┬───────┬──────────┬─────────────────────┬─────────────┐
| id                                    | ready | status  | state | progress | table               | index       |
├───────────────────────────────────────┼───────┼─────────┼───────┼──────────┼─────────────────────┼─────────────┤
| ydb://buildindex/7?id=281489389055514 | true  | SUCCESS | Done  | 100.00%  | /my-database/series | idx_release |
└───────────────────────────────────────┴───────┴─────────┴───────┴──────────┴─────────────────────┴─────────────┘

Next page token: 0
```
