# {{ ydb-short-name }} CLI commands

General syntax for calling {{ ydb-short-name }} CLI commands:

```bash
{{ ydb-cli }} [global options] <command> [<subcommand> ...] [command options]
```

where:

- `{{ ydb-cli}}` is the command to run the {{ ydb-short-name }}CLI from the OS command line.
- `[global options]` are [global options](../commands/global-options.md) that are common for all {{ ydb-short-name }} CLI commands.
- `<command>` is the command.
- `[<subcomand> ...]` are subcommands specified if the selected command contains subcommands.
- `[command options]` are command options specific to each command and subcommands.

## Commands {#list}

You can learn about the necessary commands by selecting the subject section in the menu on the left or using the alphabetical list below.

Any command can be run from the command line with the `--help` option to get help on it. You can get a list of all commands supported by the {{ ydb-short-name }} CLI by running the {{ ydb-short-name }} CLI with the `--help` option, but [without any command](../commands/service.md).

| Command / subcommand | Brief description |
--- | ---
| [config info](../commands/config-info.md) | Displaying [connection parameters](../connect.md) |
| [config profile activate](../profile/activate.md) | Activating a [profile](../profile/index.md) |
| [config profile create](../profile/create.md) | Creating a [profile](../profile/index.md) |
| [config profile delete](../profile/create.md) | Deleting a [profile](../profile/index.md) |
| [config profile get](../profile/list-and-get.md) | Getting parameters of a [profile](../profile/index.md) |
| [config profile list](../profile/list-and-get.md) | List of [profiles](../profile/index.md) |
| [config profile set](../profile/activate.md) | Activating a [profile](../profile/index.md) |
| [discovery list](../commands/discovery-list.md) | List of endpoints |
| [discovery whoami](../commands/discovery-whoami.md) | Authentication |
| [export s3](../export-import/export-s3.md) | Exporting data to S3 storage |
| [import file csv](../export-import/import-file.md) | Importing data from a CSV file |
| [import file tsv](../export-import/import-file.md) | Importing data from a TSV file |
| [import s3](../export-import/import-s3.md) | Importing data from S3 storage |
| [init](../profile/create.md) | Initializing the CLI, creating a [profile](../profile/index.md) |
| [operation cancel](../operation-cancel.md) | Aborting long-running operations |
| [operation forget](../operation-forget.md) | Deleting long-running operations from the list |
| [operation get](../operation-get.md) | Status of long-running operations |
| [operation list](../operation-list.md) | List of long-running operations |
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
| [scripting yql](../scripting-yql.md) | Executing a YQL script |
| table attribute add | Adding a table attribute |
| table attribute drop | Deleting a table attribute |
| [table drop](../table-drop.md) | Deleting a table |
| [table index add global-async](../commands/secondary_index.md#add) | Adding an asynchronous index |
| [table index add global-sync](../commands/secondary_index.md#add) | Adding a synchronous index |
| [table index drop](../commands/secondary_index.md#drop) | Deleting an index |
| [table query execute](../table-query-execute.md) | Executing a YQL query |
| [table query explain](../commands/explain-plan.md) | YQL query execution plan |
| [table read](../commands/readtable.md) | Streaming table reads |
| [table ttl set](../table-ttl-set.md) | Setting TTL parameters |
| [table ttl reset](../table-ttl-reset.md) | Resetting TTL parameters |
| [tools copy](../tools-copy.md) | Copying tables |
| [tools dump](../export-import/tools-dump.md) | Dumping a directory or table to the file system |
| [tools rename](../commands/tools/rename.md) | Renaming tables |
| [tools restore](../export-import/tools-restore.md) | Restoring data from the file system |
| [topic create](../topic-create.md) | Creating a topic |
| [topic alter](../topic-alter.md) | Updating topic parameters and consumers |
| [topic drop](../topic-drop.md) | Deleting a topic |
| [topic consumer add](../topic-consumer-add.md) | Adding a consumer to a topic |
| [topic consumer drop](../topic-consumer-drop.md) | Deleting a consumer from a topic |
| [topic consumer offset commit](../topic-consumer-offset-commit.md) | Saving a consumer offset |
| [topic read](../topic-read.md) | Reading messages from a topic |
| [topic write](../topic-write.md) | Writing messages to a topic |
{% if ydb-cli == "ydb" %}
[update](../commands/service.md) | Update the {{ ydb-short-name }} CLI
[version](../commands/service.md) | Output details about the {{ ydb-short-name }} CLI version
{% endif %}
[workload](../commands/workload/index.md) | Generate the workload
[yql](../yql.md) | Execute a YQL script (with streaming support)
