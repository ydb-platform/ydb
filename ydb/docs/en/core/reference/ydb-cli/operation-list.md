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
  * `export/yt`: The export to YT operations.
  * `export/s3`: The export to S3 operations.
  * `scriptexec`: The script execution operations.
  * `import/s3`: The import from S3 operations.
  * `incbackup`: The incremental backup operations. See also [backup collections operations guide](export-import/backup-collections/operations.md).
  * `restore`: The restore operations.

View a description of the command to get a list of long-running operations:

```bash
{{ ydb-cli }} operation list --help
```

## Parameters of the subcommand {#options}

| Name                 | Description                                                                                                                                                                                                                                                                                            |
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-s`, `--page-size`  | Number of operations on one page. If the list of operations contains more strings than specified in the `--page-size` parameter, the result will be split into several pages. To get the next page, specify the `--page-token` parameter.                                                              |
| `-t`, `--page-token` | Page token.                                                                                                                                                                                                                                                                                            |
| `--format`           | Input format.<br/>Default value: `pretty`.<br/>Acceptable values:<ul><li>`pretty`: A human-readable format.</li><li>`proto-json-base64`: Protobuf result in [JSON](https://en.wikipedia.org/wiki/JSON) format, binary strings are encoded in [Base64](https://en.wikipedia.org/wiki/Base64).</li></ul> |

## Examples {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Get a list of long-running build index operations for the `series` table:

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
