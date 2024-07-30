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
   * `export/s3`: The export operations.
   * `import/s3`: The import operations.

View a description of the command to get a list of long-running operations:

```bash
{{ ydb-cli }} operation list --help
```

## Parameters of the subcommand {#options}

| Name | Description |
---|---
| `-s`, `--page-size` | Number of operations on one page. If the list of operations contains more strings than specified in the `--page-size` parameter, the result will be split into several pages. To get the next page, specify the `--page-token` parameter. |
| `-t`, `--page-token` | Page token. |
| `--format` | Input format.<br/>Default value: `pretty`.<br/>Acceptable values:<ul><li>`pretty`: A human-readable format.</li><li>`proto-json-base64`: Protobuf result in [JSON] format{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}, binary strings are encoded in [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}.</li></ul> |

## Examples {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Get a list of long-running build index operations for the `series` table:

```bash
ydb -p quickstart operation list \
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
