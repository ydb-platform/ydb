# Canceling long-running operations

Use the `ydb operation cancel` subcommand to cancel the specified long-running operation. Only an incomplete operation can be canceled.

General format of the command:

```bash
{{ ydb-cli }} [global options...] operation cancel <id>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `id`: The ID of the long-running operation. The ID contains characters that can be interpreted by your command shell. If necessary, use shielding, for example, `'<id>'` for bash.

View a description of the command to obtain the status of a long-running operation:

```bash
{{ ydb-cli }} operation cancel --help
```

## Examples {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}
