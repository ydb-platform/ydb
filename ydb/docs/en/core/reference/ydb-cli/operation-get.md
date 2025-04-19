# Obtaining the status of long-running operations

Use the `ydb operation get` subcommand to obtain the status of the specified long-running operation.

General format of the command:

```bash
{{ ydb-cli }} [global options...] operation get [options...] <id>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `id`: The ID of the long-running operation. The ID contains characters that can be interpreted by your command shell. If necessary, use shielding, for example, `'<id>'` for bash.

View a description of the command to obtain the status of a long-running operation:

```bash
{{ ydb-cli }} operation get --help
```

## Parameters of the subcommand {#options}

| Name | Description |
---|---
| `--format` | Input format.<br/>Default value: `pretty`.<br/>Acceptable values:<ul><li>`pretty`: A human-readable format.</li><li>`proto-json-base64`: Protobuf result in [JSON] format{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}, binary strings are encoded in [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}.</li></ul> |

## Examples {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Obtain the status of the long-running operation with the `ydb://buildindex/7?id=281489389055514` ID:

```bash
ydb -p quickstart operation get \
  'ydb://buildindex/7?id=281489389055514'
```

Result:

```text
┌───────────────────────────────────────┬───────┬─────────┬───────┬──────────┬─────────────────────┬─────────────┐
| id                                    | ready | status  | state | progress | table               | index       |
├───────────────────────────────────────┼───────┼─────────┼───────┼──────────┼─────────────────────┼─────────────┤
| ydb://buildindex/7?id=281489389055514 | true  | SUCCESS | Done  | 100.00%  | /my-database/series | idx_release |
└───────────────────────────────────────┴───────┴─────────┴───────┴──────────┴─────────────────────┴─────────────┘
```
