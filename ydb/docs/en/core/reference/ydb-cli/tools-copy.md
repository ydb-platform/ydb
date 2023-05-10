# Copying tables

Using the `tools copy` subcommand, you can create a copy of one or more DB tables. The copy operation leaves the source table unchanged while the copy contains all the source table data.

General format of the command:

```bash
{{ ydb-cli }} [global options...] tools copy [options...]
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).

View a description of the command to copy a table:

```bash
{{ ydb-cli }} tools copy --help
```

## Parameters of the subcommand {#options}

| Parameter name | Parameter description |
---|---
| `--timeout` | The time within which the operation should be completed on the server. |
| `--item <property>=<value>,...` | Operation properties. You can specify the parameter more than once to copy several tables in a single transaction.<br/>Required properties:<ul><li>`destination`, `dst`, `d`: Path to target table. If the destination path contains folders, they must be created in advance. No table with the destination name should exist.</li><li>`source`, `src`, `s`: Path to source table.</li></ul> |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Create the `backup` folder in the DB:

```bash
{{ ydb-cli }} -p quickstart scheme mkdir backup
```

Copy the `series` table to a table called `series-v1`, the `seasons` table to a table called `seasons-v1`, and `episodes` to `episodes-v1` in the `backup` folder:

```bash
{{ ydb-cli }} -p quickstart tools copy --item destination=backup/series-v1,source=series --item destination=backup/seasons-v1,source=seasons --item destination=backup/episodes-v1,source=episodes
```

View the listing of objects in the `backup` folder:

```bash
{{ ydb-cli }} -p quickstart scheme ls backup
```

Result:

```text
episodes-v1  seasons-v1  series-v1
```
