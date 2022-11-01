# Deleting long running operations from the list

Use the `ydb operation forget` subcommand to delete information about the specified long running operation from the list. The operation must be complete.

General format of the command:

```bash
{{ ydb-cli }} [global options...] operation forget <id>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `id`: The ID of the long running operation. The ID contains characters that can be interpreted by your command shell. If necessary, use shielding, for example, `'<id>'` for bash.

View a description of the command to delete information about the specified long running operation:

```bash
{{ ydb-cli }} operation forget --help
```

## Examples {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Delete the long running operation with the `ydb://buildindex/7?id=281489389055514` ID from the list:

```bash
ydb -p db1 operation forget \
  'ydb://buildindex/7?id=281489389055514'
```
