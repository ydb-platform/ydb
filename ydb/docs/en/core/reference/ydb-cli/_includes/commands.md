# {{ ydb-short-name }} CLI commands

General syntax for calling {{ ydb-short-name }} CLI commands:

```bash
{{ ydb-cli }} [global options] <command> [<subcommand> ...] [command options]
```

where:

- `{{ ydb-cli}}` is the command to run the {{ ydb-short-name }} CLI from the OS command line.
- `[global options]` are [global options](../commands/global-options.md) that are common for all {{ ydb-short-name }} CLI commands.
- `<command>` is the command.
- `[<subcomand> ...]` are subcommands specified if the selected command contains subcommands.
- `[command options]` are command options specific to each command and subcommands.

## Commands {#list}

You can learn about the necessary commands by selecting the subject section in the menu on the left or using the alphabetical list below.

Any command can be run from the command line with the `--help` option to get help on it. You can get a list of all commands supported by the {{ ydb-short-name }} CLI by running the {{ ydb-short-name }} CLI with the `--help` option, but [without any command](../commands/service.md).

Command / subcommand | Brief description
--- | ---
| [admin cluster config fetch](../commands/configuration/cluster/fetch.md) | Getting the current dynamic configuration of the [cluster](../../../concepts/glossary.md#cluster) |
| [admin cluster config generate](../commands/configuration/cluster/generate.md) | Generating a dynamic configuration from a static startup configuration |
| [admin cluster config replace](../commands/configuration/cluster/replace.md) | Replacing the [cluster](../../../concepts/glossary.md#cluster) dynamic configuration |
| admin cluster config resolve | Computing the final [cluster](../../../concepts/glossary.md#cluster) dynamic configuration based on the base configuration and override selectors |
| admin cluster config version | Displaying the [cluster](../../../concepts/glossary.md#cluster) configuration version on nodes |
| [admin cluster dump](../export-import/tools-dump.md#cluster) | Dumping cluster' metadata to the file system |
| [admin cluster restore](../export-import/tools-restore.md#cluster) | Restoring cluster' metadata from the file system |
| admin database config fetch | Getting the current dynamic configuration of the [database](../../../concepts/glossary.md#database) |
| admin database config generate | Generating a [database](../../../concepts/glossary.md#database) dynamic configuration from a static startup configuration |
| admin database config replace | Replacing the [database](../../../concepts/glossary.md#database) dynamic configuration |
| admin database config resolve | Computing the final [database](../../../concepts/glossary.md#database) dynamic configuration based on the base configuration and override selectors |
| admin database config version | Displaying the [database](../../../concepts/glossary.md#database) configuration version |
| [admin database dump](../export-import/tools-dump.md#db) | Dumping database' data and metadata to the file system |
| [admin database restore](../export-import/tools-restore.md#db) | Restoring database' data and metadata from the file system |
| [admin node config init](../commands/configuration/node/init.md) | Initializing [node](../../../concepts/glossary.md#node) configuration |
| auth get-token | Getting an [authentication token](../../../concepts/glossary.md#auth-token) from authentication parameters |
| [config info](../commands/config-info.md) | Viewing [connection parameters](../connect.md) |
|| [config profile activate](../profile/activate.md) | Activating a [profile](../profile/index.md) |
|| [config profile create](../profile/create.md) | Creating a [profile](../profile/index.md) |
|| [config profile delete](../profile/create.md) | Deleting a [profile](../profile/index.md) |
|| [config profile deactivate](../profile/activate.md) | Deactivating the current active [profile](../profile/index.md) |
|| [config profile get](../profile/list-and-get.md) | Getting parameters of a [profile](../profile/index.md) |
|| [config profile list](../profile/list-and-get.md) | List of [profiles](../profile/index.md) |
|| [config profile replace](../profile/create.md) | Creating or replacing a [profile](../profile/index.md) with new parameter values |
|| [config profile set](../profile/activate.md) | Activating a [profile](../profile/index.md) |
|| [config profile update](../profile/create.md) | Updating an existing [profile](../profile/index.md) |
|| debug latency | Checking baseline latency with a variable number of parallel requests |
|| debug ping | Checking {{ ydb-short-name }} availability |
|| [discovery list](../commands/discovery-list.md) | List of endpoints |
|| [discovery whoami](../commands/discovery-whoami.md) | Authentication check |
|| [export s3](../export-import/export-s3.md) | Exporting data to S3 storage |
|| [import file csv](../export-import/import-file.md) | Importing data from a CSV file |
|| [import file json](../export-import/import-file.md) | Importing data from a JSON file |
|| [import file parquet](../export-import/import-file.md) | Importing data from a Parquet file |
|| [import file tsv](../export-import/import-file.md) | Importing data from a TSV file |
|| [import s3](../export-import/import-s3.md) | Importing data from S3 storage |
|| [init](../profile/create.md) | Initializing the CLI, creating a [profile](../profile/index.md) |
|| [monitoring healthcheck](../commands/monitoring-healthcheck.md) | Health check |
|| [operation cancel](../operation-cancel.md) | Aborting long-running operations |
|| [operation forget](../operation-forget.md) | Deleting long-running operations from the list |
|| [operation get](../operation-get.md) | Status of long-running operations |
|| [operation list](../operation-list.md) | List of long-running operations |
|| [scheme describe](../commands/scheme-describe.md) | Description of a data schema object |
|| [scheme ls](../commands/scheme-ls.md) | List of data schema objects |
|| [scheme mkdir](../commands/dir.md#mkdir) | Creating a directory |
|| [scheme permissions chown](../commands/scheme-permissions.md#chown) | Change object owner |
|| [scheme permissions clear](../commands/scheme-permissions.md#clear) | Clear permissions |
|| [scheme permissions grant](../commands/scheme-permissions.md#grant-revoke) | Grant permission |
|| [scheme permissions revoke](../commands/scheme-permissions.md#grant-revoke) | Revoke permission |
|| [scheme permissions set](../commands/scheme-permissions.md#set) | Set permissions |
|| [scheme permissions list](../commands/scheme-permissions.md#list) | View permissions |
|| [scheme permissions clear-inheritance](../commands/scheme-permissions.md#clear-inheritance) | Disable permission inheritance |
|| [scheme permissions set-inheritance](../commands/scheme-permissions.md#set-inheritance) | Enable permission inheritance |
|| [scheme rmdir](../commands/dir.md#rmdir) | Deleting a directory |
|| [scripting yql](../scripting-yql.md) | Executing a YQL script (deprecated, use [`ydb sql`](../sql.md)) |
|| [sql](../sql.md) | Execute any query |
|| [table attribute add](../table-attribute-add.md) | Adding an attribute for a row-oriented or column-oriented [table](../../../concepts/glossary.md#table) |
|| [table attribute drop](../table-attribute-drop.md) | Deleting an attribute from a row-oriented or column-oriented [table](../../../concepts/glossary.md#table) |
|| [table drop](../table-drop.md) | Deleting a row-oriented or column-oriented [table](../../../concepts/glossary.md#table) |
|| [table index add global-async](../commands/secondary_index.md#add) | Adding an asynchronous [secondary index](../../../concepts/glossary.md#secondary-index) for row-oriented [tables](../../../concepts/glossary.md#row-oriented-table) |
|| [table index add global-sync](../commands/secondary_index.md#add) | Adding a synchronous [secondary index](../../../concepts/glossary.md#secondary-index) for row-oriented [tables](../../../concepts/glossary.md#row-oriented-table) |
|| [table index drop](../commands/secondary_index.md#drop) | Deleting a [secondary index](../../../concepts/glossary.md#secondary-index) from row-oriented [tables](../../../concepts/glossary.md#row-oriented-table) |
|| [table index rename](../commands/secondary_index.md#rename) | Renaming a [secondary index](../../../concepts/glossary.md#secondary-index) for the specified [table](../../../concepts/glossary.md#table) |
|| [table query execute](../table-query-execute.md) | Executing a YQL query (deprecated, use [`ydb sql`](../sql.md)) |
|| [table query explain](../commands/explain-plan.md) | YQL query execution plan (deprecated, use [`ydb sql --explain`](../sql.md)) |
|| [table read](../commands/readtable.md) | Streaming row [table](../../../concepts/glossary.md#row-oriented-table) reads |
|| [table ttl set](../table-ttl-set.md) | Setting [TTL](../../../concepts/glossary.md#ttl) parameters for row-oriented and column-oriented [tables](../../../concepts/glossary.md#table) |
|| [table ttl reset](../table-ttl-reset.md) | Resetting [TTL](../../../concepts/glossary.md#ttl) parameters for row-oriented and column-oriented [tables](../../../concepts/glossary.md#table) |
|| [tools copy](../tools-copy.md) | Copying [tables](../../../concepts/glossary.md#table) |
|| [tools dump](../export-import/tools-dump.md#schema-objects) | Dumping individual schema objects to the file system |
|| [tools infer csv](../tools-infer.md) | Generate a `CREATE TABLE` SQL query from a CSV file |
{% if ydb-cli == "ydb" %}
|| [tools pg-convert](../../../postgresql/import.md#pg-convert) | Converting a PostgreSQL dump obtained with the pg_dump utility into a format understood by YDB |
{% endif %}
|| [tools rename](../commands/tools/rename.md) | Renaming row-oriented [tables](../../../concepts/glossary.md#row-oriented-table) |
|| [tools restore](../export-import/tools-restore.md#schema-objects) | Restoring individual schema objects from the file system |
|| [topic create](../topic-create.md) | Creating a [topic](../../../concepts/glossary.md#topic) |
|| [topic alter](../topic-alter.md) | Updating [topic](../../../concepts/glossary.md#topic) parameters and [consumers](../../../concepts/glossary.md#consumer) |
|| [topic drop](../topic-drop.md) | Deleting a [topic](../../../concepts/glossary.md#topic) |
|| [topic consumer add](../topic-consumer-add.md) | Adding a [consumer](../../../concepts/glossary.md#consumer) to a [topic](../../../concepts/glossary.md#topic) |
|| topic consumer describe | Describing a [topic](../../../concepts/glossary.md#topic) [consumer](../../../concepts/glossary.md#consumer) |
|| [topic consumer drop](../topic-consumer-drop.md) | Deleting a [consumer](../../../concepts/glossary.md#consumer) from a [topic](../../../concepts/glossary.md#topic) |
|| [topic consumer offset commit](../topic-consumer-offset-commit.md) | Saving a reading [offset](../../../concepts/glossary.md#offset) |
|| [topic read](../topic-read.md) | Reading messages from a [topic](../../../concepts/glossary.md#topic) |
|| [topic write](../topic-write.md) | Writing messages to a [topic](../../../concepts/glossary.md#topic) |
{% if ydb-cli == "ydb" %}
|| [update](../commands/service.md) | Update the {{ ydb-short-name }} CLI |
|| [version](../commands/service.md) | Output details about the {{ ydb-short-name }} CLI version |
{% endif %}
|| [workload clickbench init](../workload-click-bench.md#init) | Creating and initializing [tables](../../../concepts/glossary.md#table) for the `Clickbench` workload |
|| [workload clickbench import files](../workload-click-bench.md#load) | Loading the `Clickbench` dataset from files |
|| [workload clickbench run](../workload-click-bench.md#run) | Running the `Clickbench` benchmark |
|| [workload clickbench clean](../workload-click-bench.md#cleanup) | Deleting [tables](../../../concepts/glossary.md#table) created during the `Clickbench` workload initialization |
|| [workload kv init](../workload-kv.md#init) | Creating and initializing [tables](../../../concepts/glossary.md#table) for the `Key-Value` workload |
|| [workload kv run upsert](../workload-kv.md#upsert-kv) | Inserting random tuples into a [table](../../../concepts/glossary.md#table) using `UPSERT` in the `Key-Value` workload |
|| [workload kv run insert](../workload-kv.md#insert-kv) | Inserting random tuples into a [table](../../../concepts/glossary.md#table) using `INSERT` in the `Key-Value` workload |
|| [workload kv run mixed](../workload-kv.md#mixed-kv) | Simultaneous insertion and reading of tuples with verification of successfully read data in the `Key-Value` workload |
|| [workload kv run read-rows](../workload-kv.md#read-rows-kv) | Executing ReadRows queries returning rows by exact [primary key](../../../concepts/glossary.md#primary-key) match in the `Key-Value` workload |
|| [workload kv run select](../workload-kv.md#select-kv) | Selecting data returning rows by exact [primary key](../../../concepts/glossary.md#primary-key) match in the `Key-Value` workload |
|| [workload kv clean](../workload-kv.md#clean) | Deleting [tables](../../../concepts/glossary.md#table) created during the `Key-Value` workload initialization |
|| workload log init | Creating and initializing [tables](../../../concepts/glossary.md#table) for the `Log` workload |
|| workload log import generator | Random data generator in the `Log` workload |
|| workload log run bulk_upsert | Bulk inserting random rows into a [table](../../../concepts/glossary.md#table) near the current time in the `Log` workload |
|| workload log run delete | Deleting random rows from a [table](../../../concepts/glossary.md#table) near the current time in the `Log` workload |
|| workload log run insert | Inserting random rows into a [table](../../../concepts/glossary.md#table) near the current time in the `Log` workload using `INSERT` |
|| workload log run upsert | Inserting random rows into a [table](../../../concepts/glossary.md#table) near the current time in the `Log` workload using `UPSERT` |
|| workload log run select | Executing a set of analytical queries for log analysis: record counting, aggregation by levels, services, and components, metadata analysis, and time ranges in the `Log` workload |
|| workload log clean | Deleting [tables](../../../concepts/glossary.md#table) created during the `Log` workload initialization |
|| workload mixed init | Creating and initializing [tables](../../../concepts/glossary.md#table) for the `Mixed` workload |
|| workload mixed run bulk_upsert | Bulk inserting random rows into a [table](../../../concepts/glossary.md#table) near the current time using `BULK_UPSERT` in the `Mixed` workload |
|| workload mixed run insert | Inserting random rows into a [table](../../../concepts/glossary.md#table) near the current time using `INSERT` in the `Mixed` workload |
|| workload mixed run upsert | Updating random rows in a [table](../../../concepts/glossary.md#table) near the current time using `UPSERT` in the `Mixed` workload |
|| workload mixed run select | Selecting random rows from a [table](../../../concepts/glossary.md#table) in the `Mixed` workload |
|| workload mixed clean | Deleting [tables](../../../concepts/glossary.md#table) created during the `Mixed` workload initialization |
|| [workload query init](../workload-query.md#init) | Initializing [tables](../../../concepts/glossary.md#table) and their configurations for the `Query` workload |
|| [workload query import](../workload-query.md#load) | Filling [tables](../../../concepts/glossary.md#table) with data for the `Query` workload |
|| [workload query run](../workload-query.md#run) | Starting the `Query` workload load testing |
|| [workload query clean](../workload-query.md#cleanup) | Deleting [tables](../../../concepts/glossary.md#table) used for the `Query` workload |
|| [workload stock init](../commands/workload/stock.md#init) | Creating and initializing [tables](../../../concepts/glossary.md#table) for the `Stock` workload |
|| [workload stock run add-rand-order](../commands/workload/stock.md#insert-random-order) | Inserting orders with a random ID without processing them in the `Stock` workload |
|| [workload stock run put-rand-order](../commands/workload/stock.md#submit-random-order) | Submitting random orders with processing in the `Stock` workload |
|| [workload stock run put-same-order](../commands/workload/stock.md#submit-same-order) | Submitting orders with the same products in the `Stock` workload |
|| [workload stock run rand-user-hist](../commands/workload/stock.md#get-random-customer-history) | Selecting orders for a random customer in the `Stock` workload |
|| [workload stock run user-hist](../commands/workload/stock.md#get-customer-history) | Selecting orders for the 10,000th customer in the `Stock` workload |
|| [workload stock clean](../commands/workload/stock.md#clean) | Deleting [tables](../../../concepts/glossary.md#table) created during the `Stock` workload initialization |
|| [workload topic init](../workload-topic.md#init) | Creating and initializing a [topic](../../../concepts/glossary.md#topic) for the `Topic` workload |
|| [workload topic run full](../workload-topic.md#run-full) | Performing a full load on a [topic](../../../concepts/glossary.md#topic) with simultaneous message reading and writing in the `Topic` workload |
|| [workload topic run read](../workload-topic.md#run-read) | Performing a load for reading messages from a [topic](../../../concepts/glossary.md#topic) in the `Topic` workload |
|| [workload topic run write](../workload-topic.md#run-write) | Performing a load for writing messages to a [topic](../../../concepts/glossary.md#topic) in the `Topic` workload |
|| [workload topic clean](../workload-topic.md#clean) | Deleting the [topic](../../../concepts/glossary.md#topic) created during the `Topic` workload initialization |
|| [workload tpcc init](../workload-tpcc.md#init) | Creating and initializing [tables](../../../concepts/glossary.md#table) for the `TPC-C` benchmark |
|| [workload tpcc import](../workload-tpcc.md#load) | Filling [tables](../../../concepts/glossary.md#table) with initial data for the `TPC-C` benchmark |
|| [workload tpcc check](../workload-tpcc.md#consistency_check) | Checking `TPC-C` data consistency |
|| [workload tpcc run](../workload-tpcc.md#run) | Running the `TPC-C` benchmark |
|| [workload tpcc clean](../workload-tpcc.md#cleanup) | Deleting [tables](../../../concepts/glossary.md#table) created by the `TPC-C` benchmark |
|| [workload tpcds init](../workload-tpcds.md#init) | Creating and initializing [tables](../../../concepts/glossary.md#table) for the `TPC-DS` benchmark |
|| [workload tpcds import generator](../workload-tpcds.md#load) | Generating the `TPC-DS` dataset using the built-in generator |
|| [workload tpcds run](../workload-tpcds.md#run) | Running the `TPC-DS` benchmark |
|| [workload tpcds clean](../workload-tpcds.md#cleanup) | Deleting [tables](../../../concepts/glossary.md#table) created during the `TPC-DS` workload initialization |
|| [workload tpch init](../workload-tpch.md#init) | Creating and initializing [tables](../../../concepts/glossary.md#table) for the `TPC-H` benchmark |
|| [workload tpch import generator](../workload-tpch.md#load) | Generating the `TPC-H` dataset using the built-in generator |
|| [workload tpch run](../workload-tpch.md#run) | Running the `TPC-H` benchmark |
|| [workload tpch clean](../workload-tpch.md#cleanup) | Deleting [tables](../../../concepts/glossary.md#table) created during the `TPC-H` workload initialization |
|| [workload transfer topic-to-table init](../workload-transfer.md#init) | Creating and initializing a [topic](../../../concepts/glossary.md#topic) with consumers and [tables](../../../concepts/glossary.md#table) for a load transferring data from a topic to a table |
|| [workload transfer topic-to-table run](../workload-transfer.md#run) | Starting a load reading messages from a [topic](../../../concepts/glossary.md#topic) and writing them to a [table](../../../concepts/glossary.md#table) in transactions |
|| [workload transfer topic-to-table clean](../workload-transfer.md#clean) | Deleting the [topic](../../../concepts/glossary.md#topic) and [tables](../../../concepts/glossary.md#table) created during the workload initialization |
|| workload vector init | Creating and initializing [tables](../../../concepts/glossary.md#table) for the `Vector` workload |
|| workload vector run select | Getting top-K vectors in the `Vector` workload |
|| workload vector run upsert | Upserting vector rows into a [table](../../../concepts/glossary.md#table) in the `Vector` workload |
|| workload vector clean | Deleting [tables](../../../concepts/glossary.md#table) created during the `Vector` workload initialization |
|| [yql](../yql.md) | Executing a YQL script with streaming support (deprecated, use [`ydb sql`](../sql.md)) |
