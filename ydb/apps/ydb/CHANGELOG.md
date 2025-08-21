* Removed the `--float-mode` option from the `ydb workload tpch run` and `ydb workload tpcds run` commands. Float mode is now inferred automatically from the table schema created during the `init` phase.
* Added final execute statistics to `ydb workload * run` commands.
* Fixed a bug where the `ydb import file csv command` with the `--newline-delimited` option could get stuck if the input had incorrect data.
* Fixed a bug with the progress bar display in the `ydb workload clickbench import files` command — incorrect percentage value and excessive line breaks causing duplicated progress lines.
* Fixed a bug where the `ydb workload topic write` command could crash with an `Unknown AckedMessageId` error due to an internal race condition.
* Fixed decimal type comparison in `ydb workload * run` commands.
* Changed the default logging level from `EMERGENCY` to `ERROR` for commands that support multiple verbosity levels.
* Added the `--start-offset` option to the `ydb topic read` command, which specifies a starting position for reading from the selected partition.
* Added a new paths approach in the `ydb export s3` and `ydb import s3` commands with the new `--include` option instead of the `--item` option.
* Added support for encryption features in the `ydb export s3` and `ydb import s3` commands.

## 2.24.1 ##

* Fixed a bug where the `ydb tools dump` command was skipping scheme objects of unsupported types without notification and leaving an empty directory for them.

## 2.24.0 ##

* Fixed a bug where executing the `ydb import file csv` command could hang.
* Set default storage type as `column` (was `row`) and default datetime mode as `datetime32` (was `datetime64`) in `ydb workload * init` commands.
* Added ability of `ydb workload tpch` and `ydb workload tpcds` commands to use fraction `--scale` option.
* Added `ydb workload tpcc check` subcommand, which checks TPC-C data consistency.

## 2.23.0 ##

* Added connection check and hotkeys description in interactive mode.
* Fixed `ydb tools restore` not being able to restore local backups on Windows.
* Limited width of tables in benchmark reports.
* Added `ydb workload vector select` to benchmark RPS and recall of vector index.
* Added trivial columns completion in interactive mode.
* Added the "ydb tools infer csv" command to generate a `CREATE TABLE` SQL query from a CSV file with data.
* Fix inline hints
* Added named expressions completion in interactive mode, cache schema responses.
* Added processing of special values `null`, `/dev/null`, `stdout`, `cout`, `console`, `stderr` and `cerr` of `--output` option in `ydb workload * run` command.
* Fixed bug when `ydb wokrload` commands did not work with absolute paths.
* Added TPC-C benchmark as `ydb workload tpcc`.

## 2.22.1 ##

* Fixed bug with state working in commnads `ydb workload query import` and `ydb workload clickbench import files`.
* Fixed bug with parsing certificate files from profile.

## 2.22.0 ##

* Ignore empty directories in dumps when restoring it with `ydb tools restore`. Empty directories are created for unknown scheme objects when dumping with `ydb tools dump`.
* Added support of check canonical results in `ydb workload query run` command.
* Added support of parquet format to `ydb workload query import` and `ydb workload clickbench import files` commands.
* `ydb workload query` deep reworked. Now it can be used for user defined full cycle load testing.
* Fixed an issue where the `ydb workload * clean` commands were deleting all contents from the target directory, instead of just the tables created by the init command.
* Switched highlighting engine
* Added `ydb admin cluster config verion` command to show configuration version (V1/V2) on nodes.
* Removed `--executor` option from `ydb workload run` commands. Use always `generic`.
* Added object names completion in interactive mode
* Added `--threads` option to `ydb workload clickbench run`, `ydb workload tpch run` and `ydb workload tpcds run`. It allows to send workload queries by multiple threads.

## 2.21.0 ##

* Fixed a bug where ydb cli was trying to read parameters from stdin even if it had no data.
* Add `--replace` option to `ydb tools restore` command. If enabled, scheme objects present in the backup would be dropped before restoring.
* Added date range parameters (--date-to, --date-from to support uniform PK distribution) for ydb workload log run operations including bulk_upsert, insert, and upsert
* Do not save to local backups destination tables of `ASYNC REPLICATION` and its changefeeds. It prevents duplication of changefeeds and reduces the amount of space the backup takes on disk.
* Fix `ydb operation get` not working for running operations.
* Improved `ydb import file` commands to support files with BOM (Byte Order Mark).
* Brackets are now inserted in pairs in YDB CLI interactive mode
* Added `--scale` option to `ydb workload tpch init` and `ydb workload tpcds init` commands. Sets the percentage of the benchmark's data size and workload to use, relative to full scale.
* Added "--no-discovery" option. It allows to skip discovery and use user provided endpoint to connect to YDB cluster.
* Added `--retries` to `ydb workload <clickbenh|tpch|tpcds> run` command.
* Added `--partition-size` param to `ydb workload <clickbench/tpcds/tpch> init`.
* Fixed bugs in `ydb scheme rmdir`: 1) do not try to delete subdomains, 2) order the deletion of external tables before the deletion of external data sources.
* YDB CLI help message improvements. Different display for detailed help and brief help.
* Support coordination nodes in `ydb scheme rmdir --recursive`.
* Fixed return code of command `ydb workload * run --check-canonical` for the case when benchmark query results differ from canonical ones.
* Fixed scheme error in `ydb admin cluster dump` when specifying a domain database.
* Fixed unauthorized error in `ydb admin database restore` when multiple database admins are in dump.
* Added `--min-inflight` to `ydb debug latency` command.
* Added support for multiple `-p` (percentile) params in `ydb debug latency` command.
* `ydb debug latency` outputs additional measurements for GRPC ping.

## 2.20.0 ##

* Added support for dual configuration mode in the `ydb admin cluster config fetch` command, allowing it to handle separate cluster and storage config sections.
* Add options for client certificates in SSL/TLS connections.
* Add `ydb admin node config init` command to initialize directory with node config files.
* Add `ydb admin cluster config generate` command to generate dynamic config from static config on cluster.
* Fixed memory leak in tpcds generator.
* Include external data sources and external tables in local backups (`ydb tools dump` and `ydb tools restore`). Both scheme objects are backed up as YQL creation queries saved in the `create_external_data_source.sql` and `create_external_table.sql` files respectively, which can be executed to recreate the original scheme objects.
* Fixed a bug where `ydb auth get-token` command tried to authenticate twice: while listing andpoints and while executing actual token request.
* Fixed a bug where `ydb import file csv` command was saving progress even if a batch upload had been failed.
* Include coordination nodes in local backups (`ydb tools dump` and `ydb tools restore`). Rate limiters that utilize the coordination node are saved in the coordination node's backup folder, preserving the existing path hierarchy.
* Fixed a bug where some errors could be ignored when restoring from a local backup.
* Added `ydb workload log import generator` command.
* Queries in `ydb workload run` command are now executed in random order.
* Include topics in local backups (`ydb tools dump` and `ydb tools restore`). In this release, only the settings of the topics are retained; messages are not included in the backup.
* Added `ydb admin cluster dump` and `ydb admin cluster restore` commands for dumping all cluster-level data
* Added `ydb admin database dump` and `ydb admin database restore` commands for dumping all database-level data

## 2.19.0 ##

* Added some temporary changes to experimental `ydb admin storage` command for internal usage
* Added message query text if query fails in `ydb workload run` comamnd.
* Enable view exports and imports. Views are exported as `CREATE VIEW` YQL statements which are executed on import.
* Save current stats in `ydb workload run`.
* Added message if global timeout expiried in `ydb workload run` comamnd.
* Fixed return code of `ydb workload run` comamnd.
* Added statistics output on the current progress of the query in `ydb workload` command
* Fixed a bug where arm64 YDB CLI binary was downloading amd64 binary to replace itself during `ydb update`. To update already installed binaries to the latest arm64 version, YDB CLI should be re-installed
* Fixed a bug where `ydb workload tpch import generator` and `ydb workload tpcds import generator` commands were failing due to not all tables were created
* Fixed a bug with backslashes in `ydb workload` benchmark paths on Windows
* Added CREATE TABLE text suggestion on scheme error during `ydb import file csv`
* Backup and restore of changefeeds has been added to `ydb tools dump` and `ydb tools restore`. As a result, there are changes in the backup file structure: for tables with changefeeds, a subdirectory is created for each changefeed, named after the changefeed. This subdirectory contains two files: `changefeed_description.pb`, which contains the changefeed description, and `topic_description.pb`, which contains information about the underlying topic.
* Added `--skip-checksum-validation` option to `ydb import s3` command to skip server-side checksum validation.
* Added new experimental options for `ydb debug ping` command: `--chain-length`, `--chain-work-duration`, `--no-tail-chain`.

## 2.18.0 ##

* _awaiting release ydb server 24.4_ Query plan and statistics provide additional information:
  * Expression and attributes added to various operator properties (e.g., `GroupBy`)
  * Per-operator statistics (Rows)
  * Statistics for [column-oriented tables](./concepts/datamodel/table.md#column-oriented-tables) (Rows and Bytes)
* Fixed a bug where `ydb workload * run` command could crash in `--dry-run` mode.
* Added support for views in local backups: `ydb tools dump` and `ydb tools restore`. Views are backed up as `CREATE VIEW` queries saved in the `create_view.sql` files, which can be executed to recreate the original views.
* Replaced option `--query-settings` by `--query-prefix` one in `ydb workload <workload> run`.
* Added new options to `ydb workload topic`: --tx-commit-interval and --tx-commit-messages, allowing you to specify commit interval either in milliseconds or in number of messages written.
Also now you can load test YDB topics, using wide transactions that span over all partitions in the topic. This works both in write and in end-to-end workload scenarios.
* `ydb import file csv` command now saves import progress. Relaunching import command will continue from the line it was interrupted on
* Use QueryService by default (`--executer generic`) in `ydb workload kv` and `ydb workload stock` commands
* Use parquet format instead of CSV to fill tables in `ydb workload` benchmarks
* Made `--consumer` flag in `ydb topic read` command optional. Now if this flag is not specified, reading is performed in no-consumer mode. In this mode partition IDs should be specified with `--partition-ids` option.
* Fixed a bug in `ydb import file csv` where multiple columns with escaped quotes in the same row were parsed incorrectly
* Truncate query results output in benchmarks
* Added `ydb admin cluster bootstrap` command to bootstrap automatically configured cluster

## 2.17.0 ##

* Fixed a bug in TPC-H tables schema where the `partsupp` table had incorrect list of key columns
* Enhanced parallelism of data restoring in `ydb tools restore`
* Fixed a bug where `ydb tools restore` was failing with `Too much data` if `--upload-batch-bytes` option value was set exactly to it's maximum possible value (16MiB)
* _awaiting release ydb server 25.1_ Added `ydb debug ping` command for performance and connectivity debugging

## 2.16.0 ##

* Improved throughput of `ydb import file csv` command. It is now approximately x3 times faster
* Allow running stock bench for `OLAP` shards
* Specify more clearly what concrete type of timestamp is in options in `ydb topic` commands
* Added support different timestamp formats in `ydb topic` commands
* Added `--explain-ast` option to `ydb sql` command that prints query AST
* Highlighting in interactive mode switched to common lexer using ANSI SQL syntax so ANSI queries can be highlighted correctly
* Added location printing for errors in `ydb tools restore` command
* Diffs in `ydb workload` benchmarks are now printed prettier
* Fixed progress bar in `ydb workload import` command
* Added pg syntax to tpch and tpcds benchmarks
* Fixed a bug where restoring from a backup using --import-data could fail if the partitioning of the table was changed
* In the `ydb topic write` command the `--codec` option now has default value `RAW`.
* Added log events for `ydb tools dump` and `ydb tools restore` commands
* Added `-c` option for `ydb workload tpcds run` command to compare the result with expected value and show the diff

## 2.15.0 ##

* Description is not ready yet

## 2.14.0 ##

* Description is not ready yet

## 2.13.0 ##

* Description is not ready yet

## 2.12.0 ##

* Description is not ready yet

## 2.11.0 ##

* Description is not ready yet

## 2.10.0 ##

### Features

* Added the `ydb sql` command that runs over QueryService and can execute any DML/DDL command.
* Added `notx` support for the `--tx-mode` option in `ydb table query execute`.
* Added start and end times for long-running operation descriptions (export, import).
* Added replication description support in the `ydb scheme describe` and `ydb scheme ls` commands.
* Added big datetime types support: `Date32`, `Datetime64`, `Timestamp64`, `Interval64`.
* `ydb workload` commands rework:

  * Added the `--clear` option to the `init` subcommand, allowing tables from previous runs to be removed before workload initialization.
  * Added the `ydb workload * import` command to prepopulate tables with initial content before executing benchmarks.

### Backward incompatible changes

* `ydb workload` commands rework:

  * The `--path` option was moved to a specific workload level. For example: `ydb workload tpch --path some/tables/path init ...`.
  * The `--store=s3` option was changed to `--store=external-s3` in the `init` subcommand.


### Bug fixes

* Fixed colors in the `PrettyTable` format

## 2.9.0 ##

### Features

* Improved query logical plan tables: added colors, more information, fixed some bugs.
* The verbose option `-v` is supported for `ydb workload` commands to provide debug information.
* Added an option to run `ydb workload tpch` with an S3 source to measure [federated queries](concepts/federated_query/index.md) performance.
* Added the `--rate` option for `ydb workload` commands to control the transactions (or requests) per second limit.
* Added the `--use-virtual-addressing` option for S3 import/export, allowing the switch to [virtual hosting of buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html) for the S3 path layout.
* Improved `ydb scheme ls` performance due to listing directories in parallel.

### Bug fixes

* Resolved an issue where extra characters were truncated during line transfers in CLI tables.
* Fixed invalid memory access in `tools restore`.
* Fixed the issue of the `--timeout` option being ignored in generic and scan queries, as well as in the import command.
* Added a 60-second timeout to version checks and CLI binary downloads to prevent infinite waiting.
* Minor bug fixes.

## 2.8.0 ##

### Features

* Added configuration management commands for the cluster `ydb admin config` and `ydb admin volatile-config`.
* Added support for loading PostgreSQL-compatible data types by [ydb import file csv|tsv|json](reference/ydb-cli/export-import/import-file.md) command. Only for row-oriented tables.
* Added support for directory load from an S3-compatible storage in the [ydb import s3](reference/ydb-cli/export-import/import-s3.md) command. Currently only available on Linux and Mac OS.
* Added support for outputting the results of [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb yql](reference/ydb-cli/yql.md) and [ydb scripting yql](reference/ydb-cli/scripting-yql.md) commands in the [Apache Parquet](https://parquet.apache.org/docs/) format.
* In the [ydb workload](reference/ydb-cli/commands/workload/index.md) commands, the `--executer` option has been added, which allows to specify which type of queries to use.
* Added a column with median benchmark execution time in the statistics table of the [ydb workload clickbench](reference/ydb-cli/workload-click-bench.md) command.
* **_(Experimental)_** Added the `generic` request type to the [ydb table query execute](reference/ydb-cli/table-query-execute.md) command, allowing to perform [DDL](https://en.wikipedia.org/wiki/Data_Definition_Language) and [DML](https://en.wikipedia.org/wiki/Data_Manipulation_Language) operations, return with arbitrarily-sized results and support for [MVCC](concepts/mvcc.md). The command uses an experimental API, compatibility is not guaranteed.
* **_(Experimental)_** In the `ydb table query explain` command, the `--collect-diagnostics` option has been added to collect query diagnostics and save it to a file. The command uses an experimental API, compatibility is not guaranteed.

### Bug fixes

* Fixed an error displaying tables in `pretty` format with [Unicode](https://en.wikipedia.org/wiki/Unicode) characters.

* Fixed an error substituting the wrong primary key in the command [ydb tools pg-convert](postgresql/import.md#pg-convert).

## 2.7.0 ##

### Features

* Added the [ydb tools pg-convert](postgresql/import.md#pg-convert) command, which prepares a dump obtained by the [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) utility for loading into the YDB postgres-compatible layer.
* Added the `ydb workload query` load testing command, which loads the database with [script execution queries](reference/ydb-cli/yql.md) in multiple threads.
* Added a command `ydb scheme permissions list` to list permissions.
* In the commands [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb table query explain](reference/ydb-cli/commands/explain-plan.md), [ydb yql](reference/ydb-cli/yql.md), and [ydb scripting yql](reference/ydb-cli/scripting-yql.md), the `--flame-graph` option has been added, specifying the path to the file in which you need to save the visualization of query execution statistics.
* [Special commands](reference/ydb-cli/interactive-cli.md#spec-commands) in the interactive query execution mode are now case-insensitive.
* Added validation for [special commands](reference/ydb-cli/interactive-cli.md#spec-commands) and their [parameters](reference/ydb-cli/interactive-cli.md#internal-vars).
* Added table reading in the scenario with transactions in the command [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run).
* Added the `--commit-messages` option to the command [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run), specifying the number of messages in a single transaction.
* Added the options `--only-table-in-tx` and `--only-topic-in-tx` in the command [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run), specifying restrictions on the types of queries in a single transaction.
* Added new columns `Select time` and `Upsert time` in the statistics table in the command [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run).

### Bug fixes

* Fixed an error when loading an empty JSON list by commands: [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb scripting yql](reference/ydb-cli/scripting-yql.md) and [ydb yql](reference/ydb-cli/yql.md).

## 2.6.0 ##

### Features

* Added `--path` option to [ydb workload tpch run](reference/ydb-cli/workload-tpch.md#run), which contains the path to the directory with tables created by the [ydb workload tpch init](reference/ydb-cli/workload-tpch.md#init) command.
* Added [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md) command, which loads the database with read requests from topics and write requests to the table.
* Added the option `--consumer-prefix` in the commands [ydb workload topic init](reference/ydb-cli/workload-topic.md#init), [ydb workload topic run read|full](reference/ydb-cli/workload-topic.md#run-read), specifying prefixes of consumer names.
* Added the `--partition-ids` option in the [ydb topic read](reference/ydb-cli/topic-read.md) command, which specifies a comma-separated list of topic partition identifiers to read from.
* Added support for CSV and TSV parameter formats in [YQL query](reference/ydb-cli/parameterized-queries-cli.md) execution commands.
* The [interactive mode of query execution](reference/ydb-cli/interactive-cli.md) has been redesigned. Added [new interactive mode specific commands](reference/ydb-cli/interactive-cli.md#spec-commands): `SET`, `EXPLAIN`, `EXPLAIN AST`. Added saving history between CLI launches and auto-completion of YQL queries.
* Added the command [ydb config info](reference/ydb-cli/commands/config-info.md), which outputs the current connection parameters without connecting to the database.
* Added the command [ydb workload kv run mixed](reference/ydb-cli/workload-kv.md#mixed-kv), which loads the database with write and read requests.
* The `--percentile` option in the [ydb workload topic run write|read|full](reference/ydb-cli/workload-topic.md#run-write) commands can now take floating point values.
* The default values for the `--seconds` and `--warmup` options in the [ydb workload topic run write|read|full](reference/ydb-cli/workload-topic.md#run-write) commands have been increased to 60 seconds and 5 seconds, respectively.
* Changed the default value for the `--supported-codecs` option to `RAW` in the [ydb topic create](reference/ydb-cli/topic-create.md) and [ydb topic consumer add](reference/ydb-cli/topic-consumer-add.md) commands.

### Bug fixes

* Fixed string loss when loading with the [ydb import file json](reference/ydb-cli/export-import/import-file.md) command.
* Fixed ignored statistics during the warm-up of commands [ydb workload topic run write|read|full](reference/ydb-cli/workload-topic.md#run-write).
* Fixed incomplete statistics output in the [ydb scripting yql](reference/ydb-cli/scripting-yql.md) and [ydb yql](reference/ydb-cli/yql.md) commands.
* Fixed incorrect output of progress bar in [ydb tools dump](reference/ydb-cli/export-import/tools-dump.md) and [ydb tools restore](reference/ydb-cli/export-import/tools-restore.md) commands.
* Fixed loading large files with the header in the [ydb import file csv|tsv](reference/ydb-cli/export-import/import-file.md) command.
* Fixed hanging of the [ydb tools restore --import-data](reference/ydb-cli/export-import/tools-restore.md#optional) command.
* Fixed error `Unknown value Rejected` when executing the [ydb operation list build index](reference/ydb-cli/operation-list.md) command.

## 2.5.0 ##

### Features

* For the `ydb import file` command, a parameter [--timeout](reference/ydb-cli/export-import/import-file.md#optional) has been added that specifies the time within which the operation should be performed on the server.
* Added a progress bar in commands [ydb scheme rmdir --recursive](reference/ydb-cli/commands/dir.md#rmdir) and [ydb import file](reference/ydb-cli/export-import/import-file.md).
* Added the command [ydb workload kv run read-rows](reference/ydb-cli/workload-kv.md#read-rows-kv), which loads the database with requests to read rows using a new experimental API call ReadRows (implemented only in the [main](https://github.com/ydb-platform/ydb) branch), which performs faster key reading than [select](reference/ydb-cli/workload-kv.md#select-kv).
* New parameters `--warmup-time`, `--percentile`, `--topic` have been added to the [ydb workload topic](reference/ydb-cli/workload-topic.md), setting the test warm-up time, the percentile in the statistics output and the topic name, respectively.
* Added the [ydb workload tpch](reference/ydb-cli/workload-tpch.md) command to run the TPC-H benchmark.
* Added the `--ordered` flag in the command [ydb tools dump](reference/ydb-cli/export-import/tools-dump.md), which preserves the order by primary key in tables.

### Performance

* The data loading speed in the `ydb import file` command has been increased by adding parallel loading. The number of threads is set by the new parameter [--threads](reference/ydb-cli/export-import/import-file.md#optional).
* A performance of the [ydb import file json](reference/ydb-cli/export-import/import-file.md) command has been increased by reducing the number of data copies.

## 2.4.0 ##

### Features

* Added the ability to upload multiple files in parallel with the command [ydb import file](reference/ydb-cli/export-import/import-file.md#multiple-files).
* Added support for deleting column tables for the command [ydb scheme rmdir --recursive](reference/ydb-cli/commands/dir.md#rmdir).
* Improved stability of the command [ydb workload topic](reference/ydb-cli/workload-topic.md).

## 2.3.0 ##

### Features

* Added the interactive mode of query execution. To switch to the interactive mode, run [ydb yql](reference/ydb-cli/yql.md) without arguments. This mode is experimental: backward compatibility is not guaranteed yet.
* Added the [ydb index rename](reference/ydb-cli/commands/secondary_index.md#rename) command for [atomic replacement](dev/secondary-indexes.md#atomic-index-replacement) or renaming of a secondary index.
* Added the `ydb workload topic` command for generating the load that reads messages from topics and writes messages to topics.
* Added the [--recursive](reference/ydb-cli/commands/dir.md#rmdir-options) option for the `ydb scheme rmdir` command. Use it to delete a directory recursively, with all its content.
* Added support for the `topic` and `coordination node` types in the [ydb scheme describe](reference/ydb-cli/commands/scheme-describe.md) command.
* Added the [--commit](reference/ydb-cli/topic-read.md#osnovnye-opcionalnye-parametry) option for the `ydb topic consumer` command. Use it to commit messages you have read.
* Added the [--columns](reference/ydb-cli/export-import/import-file.md#optional) option for the `ydb import file csv|tsv` command. Use it as an alternative to the file header when specifying a column list.
* Added the [--newline-delimited](reference/ydb-cli/export-import/import-file.md#optional) option for the `ydb import file csv|tsv` command. Use it to make sure that your data is newline-free. This option streamlines import by reading data from several file sections in parallel.

### Bug fixes

* Fixed the bug that resulted in excessive memory and CPU utilization when executing the `ydb import file` command.

## 2.2.0 ##

### Features

* Fixed the error that didn't allow specifying supported compression algorithms when adding a topic consumer.
* Added support for streaming YQL scripts and queries based on options [transferred via `stdin`](reference/ydb-cli/parameterized-queries-cli.md).
* You can now [use a file](reference/ydb-cli/parameterized-queries-cli.md) to provide YQL query options
* Password input requests are now output to `stderr` instead of `stdout`.
* You can now save the root CA certificate path in a [profile](reference/ydb-cli/profile/index.md).
* Added a global option named [--profile-file](reference/ydb-cli/commands/global-options.md#service-options) to use the specified file as storage for profile settings.
* Added a new type of load testing: [ydb workload clickbench](reference/ydb-cli/workload-click-bench).

## 2.1.1 ##

### Improvements

* Added support for the `--stats` option of the [ydb scheme describe](reference/ydb-cli/commands/scheme-describe.md) command for column-oriented tables.
* Added support for Parquet files to enable their import with the [ydb import](reference/ydb-cli/export-import/import-file.md) command.
* Added support for additional logging and retries for the [ydb import](reference/ydb-cli/export-import/import-file.md) command.

## 2.1.0 ##

### Features

* You can now [create a profile non-interactively](reference/ydb-cli/profile/create.md#cmdline).
* Added the [ydb config profile update](reference/ydb-cli/profile/create.md#update) and [ydb config profile replace](reference/ydb-cli/profile/create.md#replace) commands to update and replace profiles, respectively.
* Added the `-1` option for the [ydb scheme ls](reference/ydb-cli/commands/scheme-ls.md) command to enable output of a single object per row.
* You can now save the IAM service URL in a profile.
* Added support for username and password-based authentication without specifying the password.
* Added support for AWS profiles in the [ydb export s3](reference/ydb-cli/export-import/auth-s3.md#auth) command.
* You can now create profiles using `stdin`. For example, you can pass the [YC CLI](https://yandex.cloud/docs/cli/) `yc ydb database get information` command output to the `ydb config profile create` command input.

### Bug fixes

* Fixed the error when request results were output in JSON-array format incorrectly if they included multiple server responses.
* Fixed the error that disabled profile updates so that an incorrect profile was used.

## 2.0.0 ##

### Features

* Added the ability to work with topics:

  * `ydb topic create`: Create a topic.
  * `ydb topic alter`: Update a topic.
  * `ydb topic write`: Write data to a topic.
  * `ydb topic read`: Read data from a topic.
  * `ydb topic drop`: Delete a topic.

* Added a new type of load testing:

  * `ydb workload kv init`: Create a table for kv load testing.
  * `ydb workload kv run`: Apply one of three types of load: run multiple `UPSERT` sessions, run multiple `INSERT` sessions, or run multiple sessions of GET requests by primary key.
  * `ydb workload kv clean`: Delete a test table.

* Added the ability to disable current active profile (see the `ydb config profile deactivate` command).
* Added the ability to delete a profile non-interactively with no commit (see the `--force` option under the `ydb config profile remove` command).
* Added CDC support for the `ydb scheme describe` command.
* Added the ability to view the current DB status (see the `ydb monitoring healthcheck` command).
* Added the ability to view authentication information (token) to be sent with DB queries under the current authentication settings (see the `ydb auth get-token` command).
* Added the ability for the `ydb import` command to read data from stdin.
* Added the ability to import data in JSON format from a file or stdin (see the `ydb import file json` command).

### Improvements

* Improved command processing. Improved the accuracy of user input parsing and validation.

## 1.9.1 ##

### Features

* Added the ability to compress data when exporting it to S3-compatible storage (see the `--compression` option of the [ydb export s3](reference/ydb-cli/export-import/export-s3.md) command).
* Added the ability to manage new {{ ydb-short-name }} CLI version availability auto checks (see the `--disable-checks` and `--enable-checks` options of the [ydb version](reference/ydb-cli/version.md) command).
