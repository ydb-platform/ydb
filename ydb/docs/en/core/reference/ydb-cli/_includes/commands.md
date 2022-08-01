# {{ ydb-short-name }} CLI commands

General syntax for calling {{ ydb-short-name }} CLI commands:

```bash
{{ ydb-cli }} [global options] <command> [<subcommand> ...] [command options]
```

, where:

- `{{ ydb-cli}}` is the command to run the {{ ydb-short-name }} CLI from the OS command line.
- `[global options]` are [global options](../commands/global-options.md) that are common for all {{ ydb-short-name }} CLI commands.
- `<command>` is the command.
- `[<subcomand> ...]` are subcommands specified if the selected command contains subcommands.
- `[command options]` are command options specific to each command and subcommands.

## Commands {#list}

You can learn about the necessary commands by selecting the subject section in the menu on the left or using the alphabetical list below.

Any command can be run from the command line with the `--help` option to get help on it. You can get a list of all supported {{ ydb-short-name }} CLI of commands by running the {{ ydb-short-name }} CLI with the `--help` option [with no command specified](../commands/service.md).

| Command / subcommand | Brief description |
| --- | --- |
| [config profile activate](../profile/activate.md) | Activating a [profile](../profile/index.md) |
| [config profile create](../profile/create.md) | Creating a [profile](../profile/index.md) |
| [config profile delete](../profile/create.md) | Deleting a [profile](../profile/index.md) |
| [config profile get](../profile/list-and-get.md) | Getting parameters of a [profile](../profile/index.md) |
| [config profile list](../profile/list-and-get.md) | List of [profiles](../profile/index.md) |
| [config profile set](../profile/activate.md) | Activating a [profile](../profile/index.md) |
| [discovery list](../commands/discovery-list.md) | List of endpoints |
| [discovery whoami](../commands/discovery-whoami.md) | Authentication |
| [export s3](../export_import/s3_export.md) | Exporting data to S3 storage |
| [import file csv](../export_import/import-file.md) | Importing data from a CSV file |
| [import file tsv](../export_import/import-file.md) | Importing data from a TSV file |
| [import s3](../export_import/s3_import.md) | Importing data from S3 storage |
| [init](../profile/create.md) | Initializing the CLI, creating a [profile](../profile/index.md) |
| operation cancel | Aborting a background operation |
| operation forget | Removing a background operation from history |
| operation get | Background operation status |
| operation list | List of background operations |
| [scheme describe](../commands/scheme-describe.md) | Description of a data schema object |
| [scheme ls](../commands/scheme-ls.md) | List of data schema objects |
| [scheme mkdir](../commands/dir.md#mkdir) | Creating a directory |
| scheme permissions add | Granting permissions |
| scheme permissions chown | Changing the owner of an object |
| scheme permissions clear | Clearing permissions |
| scheme permissions grant | Granting permissions |
| scheme permissions remove | Removing a permission |
| scheme permissions revoke | Revoking a permission |
| scheme permissions set | Setting permissions |
| [scheme rmdir](../commands/dir.md#rmdir) | Deleting a directory |
| scripting yql | Executing a YQL script |
| table attribute add | Adding a table attribute |
| table attribute drop | Deleting a table attribute |
| table drop | Deleting a table |
| [table index add global-async](../commands/secondary_index.md#add) | Adding an asynchronous index |
| [table index add global-sync](../commands/secondary_index.md#add) | Adding a synchronous index |
| [table index drop](../commands/secondary_index.md#drop) | Deleting an index |
| [table query execute](../commands/query.md) | Executing a YQL query |
| [table query explain](../commands/explain-plan.md) | YQL query execution plan |
| [table readtable](../commands/readtable.md) | Streaming table reads |
| table ttl drop | Deleting TTL parameters |
| table ttl set | Setting TTL parameters |
| tools copy | Copying tables |
| [tools dump](../export_import/tools_dump.md) | Dumping a directory or table to the file system |
| [tools rename](../commands/tools/rename.md) | Renaming tables |
| [tools restore](../export_import/tools_restore.md) | Restoring data from the file system |
{% if ydb-cli == "ydb" %}
[update](../commands/service.md) | Updating the {{ ydb-short-name }} CLI
[version](../commands/service.md) | Displaying the version of the {{ ydb-short-name }} CLI
{% endif %}
[workload](../commands/workload/index.md) | Generating YQL load | Running a YQL script (with streaming support)

