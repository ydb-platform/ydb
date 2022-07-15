# Getting {{ ydb-short-name }} CLI version

Use the `version` subcommand to find out the version of the {{ ydb-short-name }} CLI installed and manage new version availability auto checks.

New version availability auto checks are made when you run any {{ ydb-short-name }} CLI command, except `ydb version --enable-checks` and `ydb version --disable-checks`, but only once in 24 hours. The result and time of the last check are saved to the {{ ydb-short-name }} CLI configuration file.

General format of the command:

```bash
{{ ydb-cli }} [global options...] version [options...]
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).

View a description of the command:

```bash
{{ ydb-cli }} version --help
```

## Parameters of the subcommand {#options}

| Parameter | Description |
---|---
| `--semantic` | Get only the version number. |
| `--check` | Check if a new version is available. |
| `--disable-checks` | Disable new version availability checks. |
| `--enable-checks` | Enable new version availability checks. |

## Examples {#examples}

### Disable new version availability checks {#disable-checks}

When running {{ ydb-short-name }} CLI commands, the system automatically checks if a new version is available. If the host where the command is run doesn't have internet access, this causes a delay and the corresponding warning appears during command execution. To disable auto checks for updates, run:

```bash
{{ ydb-cli }} version --disable-checks
```

Result:

```text
Latest version checks disabled
```

### Getting only the version number {#semantic}

To facilitate data handling in scripts, you can limit result to the {{ ydb-short-name }} CLI version number:

```bash
{{ ydb-cli }} version --semantic
```

Result:

```text
1.9.1
```
