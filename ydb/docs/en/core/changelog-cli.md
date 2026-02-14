# {{ ydb-short-name }} CLI changelog

## Version 2.29.0 {#2-29-0}

Released on February 11, 2026. To update to version **2.29.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Enhancements to the `{{ ydb-cli }}` [interactive mode](./reference/ydb-cli/interactive-cli.md):
  * Introduced the `/help` command for interactive command guidance.
  * Introduced the `/config` command, providing an interactive dialog to view and customize {{ ydb-short-name }} CLI settings, including:
    * Enabling or disabling autocompletion hints.
    * Enabling or disabling color output.
    * Interactively selecting a color theme from a set of predefined options, with support for cloning and customizing your own theme.
* Added a download progress bar to the `{{ ydb-cli }} update` [command](./reference/ydb-cli/commands/service.md).
* Added the `--include-index-data` option to the `{{ ydb-cli }} export s3` [command](./reference/ydb-cli/export-import/export-s3.md), enabling index data export.
* Added the `--index-population-mode` option to the `{{ ydb-cli }} import s3` [command](./reference/ydb-cli/export-import/import-s3.md), allowing selection of the index population mode (e.g., `build` or `import`).
* Added the `Created by`, `Create time`, and `End time` fields to the "build index" and "execute script" operations in the `{{ ydb-cli }} operation` [subcommands](./reference/ydb-cli/operation-list.md).
* Added unified time interval format support across {{ ydb-short-name }} CLI commands. Options accepting time durations now support explicit time units (e.g., `5s`, `2m`, `1h`) while maintaining backward compatibility with plain numbers interpreted using their original default units.
* Replaced the deprecated "Keep in memory" field with the "Cache mode" field in the column families description of the `{{ ydb-cli }} scheme describe` [command](./reference/ydb-cli/commands/scheme-describe.md).

### Improvements

* Improved the `{{ ydb-cli }} init` and `{{ ydb-cli }} config profile` [commands](./reference/ydb-cli/profile/index.md) with interactive menus.
* Improved progress bars: consistent MiB/GiB units, stable speed display, and a dual progress bar for the `{{ ydb-cli }} import file` [command](./reference/ydb-cli/export-import/import-file.md) showing both in-progress and confirmed bytes.

### Bug fixes

* Fixed an out-of-memory issue in the `{{ ydb-cli }} workload query run` [command](./reference/ydb-cli/commands/workload/index.md) for queries with large result sets.
* Fixed static credentials parsing to avoid using a [profile](./reference/ydb-cli/profile/index.md) password when the username comes from another source.

## Version 2.28.0 {#2-28-0}

Released on December 19, 2025. To update to version **2.28.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added `snapshot-ro` and `snapshot-rw` transaction modes to the `--tx-mode` option of the `{{ ydb-cli }} table query execute` [command](./reference/ydb-cli/table-query-execute.md).
* Added `NO_COLOR` environment variable support to disable ANSI colors in {{ ydb-short-name }} CLI (see [no-color.org](https://no-color.org/)).
* Added a simple progress bar for non-interactive stderr.
* Added the `omit-indexes` property to the `--item` option of the `{{ ydb-cli }} tools copy` [command](./reference/ydb-cli/tools-copy.md), allowing tables to be copied without their indexes.
* Added the `import files` subcommand to the `{{ ydb-cli }} workload vector` [command](./reference/ydb-cli/commands/workload/index.md) to populate the table from CSV or Parquet files.
* Added the `import generate` subcommand to the `{{ ydb-cli }} workload vector` [command](./reference/ydb-cli/commands/workload/index.md) to populate the table with random data.
* **_(Requires server v26.1+)_** Changes to previously added `{{ ydb-cli }} admin cluster state fetch` command:
  * Renamed to `{{ ydb-cli }} admin cluster diagnostics collect`.
  * Added the `--no-sanitize` option, which disables sanitization and preserves sensitive data in the output.
  * Added the `--output` option to specify the path to the output `.tar.bz2` file.


### Bug fixes

* Fixed a bug where the `{{ ydb-cli }} tools restore` [command](./reference/ydb-cli/export-import/tools-restore.md) could crash with a `mutex lock failure (Invalid argument)` error due to an internal race condition.
* Fixed restoration of views containing named expressions and views that access secondary indexes in the `{{ ydb-cli }} tools restore` [command](./reference/ydb-cli/export-import/tools-restore.md).

## Version 2.27.0 {#2-27-0}

Released on October 30, 2025. To update to version **2.27.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added the `--exclude` option to the `{{ ydb-cli }} import s3` [command](./reference/ydb-cli/export-import/import-s3.md), allowing schema objects to be excluded from the import if their names match a pattern.
* Added [transfer](./concepts/transfer.md) objects support to the `{{ ydb-cli }} tools dump` [command](./reference/ydb-cli/export-import/tools-dump.md) and `{{ ydb-cli }} tools restore` [command](./reference/ydb-cli/export-import/tools-restore.md).
* Added a new `--retention-period` option to the `{{ ydb-cli }} topic` subcommands. Usage of the legacy `--retention-period-hours` option is discouraged.
* The `{{ ydb-cli }} topic consumer add` [command](./reference/ydb-cli/topic-consumer-add.md) now has a new `--availability-period` option, which overrides the consumer's retention guarantee.
* The `{{ ydb-cli }} workload vector` [commands](./reference/ydb-cli/commands/workload/index.md) now support `build-index` and `drop-index` subcommands.
* **_(Requires server v26.1+)_** Added the `{{ ydb-cli }} admin cluster state fetch` command to collect information about cluster nodes' state and metrics.

### Bug fixes

* Fixed a bug where the `{{ ydb-cli }} debug ping` command crashed on any error.

## Version 2.26.0 {#2-26-0}

Released on September 25, 2025. To update to version **2.26.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added the `--no-merge` and `--no-cache` options to the `{{ ydb-cli }} monitoring healthcheck` [command](./reference/ydb-cli/commands/monitoring-healthcheck.md).
* Added query compilation time statistics to the `{{ ydb-cli }} workload * run` [commands](./reference/ydb-cli/commands/workload/index.md).
* Added the `--retries` option to the `{{ ydb-cli }} tools restore` [command](./reference/ydb-cli/export-import/tools-restore.md), allowing to set the number of retries for every upload data request.
* **_(Requires server v25.4+)_** Added the `--replace-sys-acl` option to the `{{ ydb-cli }} tools restore` [command](./reference/ydb-cli/export-import/tools-restore.md), which specifies whether to replace the ACL for system objects.

## Version 2.25.0 {#2-25-0}

Released on September 1, 2025. To update to version **2.25.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added final execute statistics to the `{{ ydb-cli }} workload * run` [commands](./reference/ydb-cli/commands/workload/index.md).
* Added the `--start-offset` option to the `{{ ydb-cli }} topic read` [command](./reference/ydb-cli/topic-read.md), which specifies a starting position for reading from the selected partition.
* **_(Requires server v25.3+)_** Added a new paths approach in the `{{ ydb-cli }} export s3` and `{{ ydb-cli }} import s3` [commands](./reference/ydb-cli/export-import/export-s3.md) with the new `--include` option instead of the `--item` option.
* **_(Requires server v25.3+)_** Added support for encryption features in the `{{ ydb-cli }} export s3` and `{{ ydb-cli }} import s3` [commands](./reference/ydb-cli/export-import/export-s3.md).
* **_(Requires server v25.3+)_** **_(Experimental)_** Added the `{{ ydb-cli }} admin cluster bridge` commands to manage a cluster in the bridge mode: `list`, `switchover`, `failover`, `takedown`, `rejoin`.

### Improvements

* User and password authentication options are now parsed independently, allowing them to be sourced from different priority levels. For example, the username can be specified via the `--user` option while the password is retrieved from the `YDB_PASSWORD` environment variable.
* Changed the default logging level from `EMERGENCY` to `WARN` for commands that support multiple verbosity levels.

### Backward incompatible changes

* Removed the `--float-mode` option from the `{{ ydb-cli }} workload tpch run` and `{{ ydb-cli }} workload tpcds run` [commands](./reference/ydb-cli/commands/workload/index.md). Float mode is now inferred automatically from the table schema created during the `init` phase.

### Bug fixes

* Fixed a bug where the `{{ ydb-cli }} import file csv` [command](./reference/ydb-cli/export-import/import-file.md) with the `--newline-delimited` option could get stuck when processing input with invalid data.
* Fixed an issue with the progress bar display in the `{{ ydb-cli }} workload clickbench import files` [command](./reference/ydb-cli/workload-click-bench.md): incorrect percentage values and excessive line breaks caused duplicate progress lines.
* Fixed a bug where the `{{ ydb-cli }} workload topic write` [command](./reference/ydb-cli/topic-write.md) could crash with an `Unknown AckedMessageId` error due to an internal race condition.
* Fixed decimal type comparison in the `{{ ydb-cli }} workload * run` [commands](./reference/ydb-cli/commands/workload/index.md).

## Version 2.24.1 {#2-24-1}

Released on July 28, 2025. To update to version **2.24.1**, select the [Downloads](downloads/ydb-cli.md) section.

### Bug fixes

* Fixed a bug where the `{{ ydb-cli }} tools dump` [command](./reference/ydb-cli/export-import/tools-dump.md#schema-objects) silently skipped schema objects of unsupported types and created empty directories for them in the destination folder on the file system.

## Version 2.24.0 {#2-24-0}

Released on July 23, 2025. To update to version **2.24.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added the ability for the `{{ ydb-cli }} workload tpch` and `{{ ydb-cli }} workload tpcds` [commands](./reference/ydb-cli/commands/workload/index.md) to use a fractional value for the `--scale` option, specifying a percentage of the full benchmark's data size and workload.
* Added the `{{ ydb-cli }} workload tpcc check` command to check TPC-C data consistency.

### Improvements

* Changed the default storage type in `{{ ydb-cli }} workload * init` [commands](./reference/ydb-cli/commands/workload/index.md) to `column` (from `row`), and the default datetime mode to `datetime32` (from `datetime64`).

### Bug fixes

* Fixed an issue where the `{{ ydb-cli }} import file csv` [command](./reference/ydb-cli/export-import/import-file.md) could get stuck during execution.

## Version 2.23.0 {#2-23-0}

Released on July 16, 2025. To update to version **2.23.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added the `{{ ydb-cli }} workload tpcc` command to run a TPC-C benchmark.
* Added the `{{ ydb-cli }} workload vector select` command to benchmark vector index performance and recall.
* Added the `{{ ydb-cli }} tools infer csv` command to generate a `CREATE TABLE` SQL query from a CSV data file.

### Improvements

* Enhanced processing of special values (`null`, `/dev/null`, `stdout`, `cout`, `console`, `stderr`, and `cerr`) for the `--output` option in the `{{ ydb-cli }} workload * run` [commands](./reference/ydb-cli/commands/workload/index.md).
* The `{{ ydb-cli }} workload` [commands](./reference/ydb-cli/commands/workload/index.md) now work with absolute paths for database scheme objects.
* Improvements in the `{{ ydb-cli }}` [interactive mode](./reference/ydb-cli/interactive-cli.md):
  * Added server connection check and hotkeys description.
  * Improved inline hints.
  * Added table column names completion.
  * Added schema caching.

### Bug fixes

* Fixed an issue where the `{{ ydb-cli }} tools restore` [command](./reference/ydb-cli/export-import/tools-restore.md) was not working on Windows.

## Version 2.22.1 {#2-22-1}

Released on June 17, 2025. To update to version **2.22.1**, select the [Downloads](downloads/ydb-cli.md) section.

### Bug fixes

* Fixed an issue where the certificate was not read from a file if the path to the file was specified in the [profile](./reference/ydb-cli/profile/index.md) with the `ca-file` field.
* Fixed an issue where the `{{ ydb-cli }} workload query import` and `{{ ydb-cli }} workload clickbench import files` [commands](./reference/ydb-cli/workload-click-bench.md) displayed number of rows instead of number of bytes in progress state.

## Version 2.22.0 {#2-22-0}

Released on June 4, 2025. To update to version **2.22.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added scheme object names completion in interactive mode.
* Enhanced the capabilities of the `{{ ydb-cli }} workload query` command: added `{{ ydb-cli }} workload query init`, `{{ ydb-cli }} workload query import`, and `{{ ydb-cli }} workload query clean` commands, and modified the `{{ ydb-cli }} workload query run` command. Using these commands, you can initialize tables, populate them with data, perform load testing, and clean up the data afterwards.
* Added the `--threads` option to the `{{ ydb-cli }} workload clickbench run`, `{{ ydb-cli }} workload tpch run`, and `{{ ydb-cli }} workload tpcds run` [commands](./reference/ydb-cli/workload-click-bench.md). This option allows to specify the number of threads sending the queries.
* **_(Requires server v25.1+)_** **_(Experimental)_** Added the `{{ ydb-cli }} admin cluster config version` [command](./reference/ydb-cli/commands/configuration/cluster/index.md#list) to show the configuration version (V1/V2) on nodes.

### Backward incompatible changes

* Removed the `--executor` option from the `{{ ydb-cli }} workload * run` [commands](./reference/ydb-cli/commands/workload/index.md). The `generic` executor is now always used.

### Bug fixes

* Fixed an issue where the `{{ ydb-cli }} workload * clean` [commands](./reference/ydb-cli/commands/workload/index.md) were deleting all contents from the target directory, instead of just the tables created by the init command.

## Version 2.21.0 {#2-21-0}

Released on May 22, 2025. To update to version **2.21.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added the `--no-discovery` [global option](./reference/ydb-cli/commands/global-options.md), allowing to skip discovery and connect to user-provided endpoint directly.
* Added new options for workload commands:
  * Added the `--scale` option to the `{{ ydb-cli }} workload tpch init` and `{{ ydb-cli }} workload tpcds init` [commands](./reference/ydb-cli/workload-tpch.md) to set the percentage of the benchmark's data size and workload to use, relative to full scale.
  * Added the `--retries` option to the `{{ ydb-cli }} workload <clickbench|tpch|tpcds> run` [commands](./reference/ydb-cli/workload-click-bench.md) to specify maximum retry count for every request.
  * Added the `--partition-size` option to the `{{ ydb-cli }} workload <clickbench|tpcds|tpch> init` [commands](./reference/ydb-cli/workload-click-bench.md) to set maximum partition size in megabytes for row tables.
  * Added date range parameters (`--date-to`, `--date-from`) to the `{{ ydb-cli }} workload log run` command to support uniform primary key distribution.
* Enhanced backup and restore functionality:
  * Added the `--replace` and `--verify-existence` options to the `{{ ydb-cli }} tools restore` [command](./reference/ydb-cli/export-import/tools-restore.md#schema-objects) to control the removal of existing objects that match those in the backup before restoration.
  * Improved the `{{ ydb-cli }} tools dump` [command](./reference/ydb-cli/export-import/tools-dump.md#schema-objects) by not saving [replica tables](./concepts/async-replication.md) with ASYNC REPLICATION and their [changefeeds](./concepts/glossary.md#changefeed) to local backups. It prevents duplication of changefeeds and reduces the amount of space the backup takes on disk.
* Enhanced CLI usability:
  * Detailed help message (`-hh`) now shows the whole subcommand tree.
  * Added automatic pair insertion for brackets in `{{ ydb-cli }}` interactive mode.
  * Added support for files with BOM (Byte Order Mark) in the `{{ ydb-cli }} import file` [commands](./reference/ydb-cli/export-import/import-file.md).
* **_(Requires server v25.1+)_** **_(Experimental)_** Improved the `{{ ydb-cli }} debug latency` command:
  * Added the `--min-inflight` parameter to set minimum number of concurrent requests (default: 1).
  * Added the `--percentile` option to specify custom latency percentiles.
  * Enhanced the output with additional gRPC ping measurements.

### Bug fixes

* The `{{ ydb-cli }} operation get` [command](./reference/ydb-cli/operation-get.md) now properly handles running operations.
* Fixed errors in the `{{ ydb-cli }} scheme rmdir` [command](./reference/ydb-cli/commands/dir.md#rmdir):
  * Fixed an issue where the command was trying to delete subdomains.
  * Fixed deletion order: external tables are now deleted before external data sources due to possible dependencies between them.
  * Added support for coordination nodes in recursive removal.
* Fixed return code of the `{{ ydb-cli }} workload * run --check-canonical` command when results differ from canonical ones.
* Fixed an issue where CLI was attempting to read parameters from stdin even without available data.
* **_(Requires server v25.1+)_** **_(Experimental)_** Fixed an authorization error in the `{{ ydb-cli }} admin database restore` [command](./reference/ydb-cli/export-import/tools-restore.md#db) when restoring from a backup containing multiple database administrator accounts.

## Version 2.20.0 {#2-20-0}

Released on March 5, 2025. To update to version **2.20.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added [topics](./concepts/datamodel/topic.md) support in the `{{ ydb-cli }} tools dump` and `{{ ydb-cli }} tools restore` [commands](./reference/ydb-cli/export-import/tools-dump.md). In this release, only topic settings are retained; messages are not included in the backup.
* Added [coordination nodes](./concepts/datamodel/coordination-node.md) support in the `{{ ydb-cli }} tools dump` and `{{ ydb-cli }} tools restore` [commands](./reference/ydb-cli/export-import/tools-dump.md).
* Added the new `{{ ydb-cli }} workload log import generator` command.
* Added new global options for client certificates in SSL/TLS connections:
  * `--client-cert-file`: File containing a client certificate for SSL/TLS connections (PKCS#12 or PEM-encoded).
  * `--client-cert-key-file`: File containing a PEM-encoded private key for the client certificate.
  * `--client-cert-key-password-file`: File containing a password for the private key (if the key is encrypted).
* Queries in the `{{ ydb-cli }} workload run` command are now executed in random order.
* **_(Requires server v25.1+)_** Added support for [external data sources](./concepts/datamodel/external_data_source.md) and [external tables](./concepts/datamodel/external_table.md) in the `{{ ydb-cli }} tools dump` and `{{ ydb-cli }} tools restore` [commands](./reference/ydb-cli/export-import/tools-dump.md).
* **_(Experimental)_** Added the `{{ ydb-cli }} admin node config init` command to initialize a directory with node configuration files.
* **_(Requires server v25.1+)_** **_(Experimental)_** Added the `{{ ydb-cli }} admin cluster config generate` [command](./reference/ydb-cli/commands/configuration/cluster/generate.md) to generate a dynamic configuration file from a cluster static configuration file.
* **_(Requires server v25.1+)_** **_(Experimental)_** Added the [command](./reference/ydb-cli/export-import/tools-dump.md#cluster) `{{ ydb-cli }} admin cluster dump` and the [command](./reference/ydb-cli/export-import/tools-restore.md#cluster) `{{ ydb-cli }} admin cluster restore` for dumping all cluster-level data. These dumps contain a list of databases with metadata, users, and groups but do not include schema objects.
* **_(Requires server v25.1+)_** **_(Experimental)_** Added the `{{ ydb-cli }} admin database dump` and `{{ ydb-cli }} admin database restore` commands for dumping all database-level data. These dumps contain database metadata, schema objects, their data, users, and groups.
* **_(Requires server v25.1+)_** **_(Experimental)_** Added the `--dedicated-storage-section` and `--dedicated-cluster-section` options to the `{{ ydb-cli }} admin cluster config fetch` command, allowing cluster and storage config sections to be fetched separately.

### Bug fixes

* Fixed a bug where the `{{ ydb-cli }} auth get-token` command attempted to authenticate twice: once while listing endpoints and again while executing the actual token request.
* Fixed a bug where the `{{ ydb-cli }} import file csv` command was saving progress even if a batch upload had failed.
* Fixed a bug where some errors could be ignored when restoring from a local backup with the `{{ ydb-cli }} tools restore` command.
* Fixed a memory leak in the data generator for the `{{ ydb-cli }} workload tpcds` benchmark.

## Version 2.19.0 {#2-19-0}

Released on February 5, 2025. To update to version **2.19.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added [changefeeds](./concepts/cdc.md) support in the `{{ ydb-cli }} tools dump` and `{{ ydb-cli }} tools restore` [commands](./reference/ydb-cli/export-import/tools-dump.md).
* Added `CREATE TABLE` text suggestion on schema error during the `{{ ydb-cli }} import file csv` [command](./reference/ydb-cli/export-import/import-file.md).
* Added statistics output on the current progress of the query in the `{{ ydb-cli }} workload` [command](./reference/ydb-cli/commands/workload/index.md).
* Added query text to the error message if a query fails in the `{{ ydb-cli }} workload run` [command](./reference/ydb-cli/commands/workload/index.md).
* Added a message if the global timeout expired in the `{{ ydb-cli }} workload run` [command](./reference/ydb-cli/commands/workload/index.md).
* **_(Requires server v25.1+)_** Added [views](./concepts/datamodel/view.md) support in the `{{ ydb-cli }} export s3` and `{{ ydb-cli }} import s3`. Views are exported as `CREATE VIEW` YQL statements, which are executed on import.
* **_(Requires server v25.1+)_** Added the `--skip-checksum-validation` option to the `{{ ydb-cli }} import s3` [command](./reference/ydb-cli/export-import/import-s3.md) to skip server-side checksum validation.
* **_(Requires server v25.1+)_** **_(Experimental)_** Added new options for the `{{ ydb-cli }} debug ping` command: `--chain-length`, `--chain-work-duration`, `--no-tail-chain`.
* **_(Requires server v25.1+)_** **_(Experimental)_** Added new options for the `{{ ydb-cli }} admin storage fetch` command: `--dedicated-storage-section` and `--dedicated-cluster-section`.
* **_(Requires server v25.1+)_** **_(Experimental)_** Added new options for the `{{ ydb-cli }} admin storage replace` command: `--filename`, `--dedicated-cluster-yaml`, `--dedicated-storage-yaml`, `--enable-dedicated-storage-section` and `--disable-dedicated-storage-section`.

### Bug fixes

* Fixed a bug where the arm64 {{ ydb-short-name }} CLI binary was downloading the amd64 binary to replace itself during the `{{ ydb-cli }} update` [command](./reference/ydb-cli/commands/service.md). To update already installed binaries to the latest arm64 version, {{ ydb-short-name }} CLI should be reinstalled.
* Fixed the return code of the `{{ ydb-cli }} workload run` [command](./reference/ydb-cli/commands/workload/index.md).
* Fixed a bug where the `{{ ydb-cli }} workload tpch import generator` and `{{ ydb-cli }} workload tpcds import generator` [commands](./reference/ydb-cli/workload-tpch.md) were failing because not all tables had been created.
* Fixed a bug with backslashes in the `{{ ydb-cli }} workload` [commands](./reference/ydb-cli/commands/workload/index.md) paths on Windows.

## Version 2.18.0 {#2-18-0}

Released on December 24, 2024. To update to version **2.18.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added support for [views](./concepts/datamodel/view) in local backups: `{{ ydb-cli }} tools dump` and `{{ ydb-cli }} tools restore`. Views are backed up as `CREATE VIEW` queries saved in the `create_view.sql` files, which can be executed to recreate the original views.
* Added new options to the `{{ ydb-cli }} workload topic run` [command](./reference/ydb-cli/workload-topic#run-write): `--tx-commit-interval` and `--tx-commit-messages`, allowing you to specify the interval between transaction commits in milliseconds or in the number of messages written, respectively.
* Made the `--consumer` flag in the `{{ ydb-cli }} topic read` [command](./reference/ydb-cli/topic-read) optional. In the non-subscriber reading mode, the partition IDs must be specified with the `--partition-ids` option. In this case, the read is performed without saving the offset commit.
* The `{{ ydb-cli }} import file csv` [command](./reference/ydb-cli/export-import/import-file.md) now saves the import progress. Relaunching the import command will resume the process from the row where it was interrupted.
* In the `{{ ydb-cli }} workload kv` and `{{ ydb-cli }} workload stock` commands, the default value of the `--executer` option has been changed to `generic`, which makes them no longer rely on the legacy query execution infrastructure.
* Replaced the CSV format with Parquet for filling tables in the `{{ ydb-cli }} workload` benchmarks.
* **_(Requires server v25.1+)_** **_(Experimental)_** Added new `{{ ydb-cli }} admin storage` command with `fetch` and `replace` subcommands to manage server storage configuration.

### Backward incompatible changes

* Replaced the `--query-settings` option with `--query-prefix` in the `{{ ydb-cli }} workload * run` command.

### Bug fixes

* Fixed a bug where the `{{ ydb-cli }} workload * run` command could crash in `--dry-run` mode.
* Fixed a bug in the `{{ ydb-cli }} import file csv` where multiple columns with escaped quotes in the same row were parsed incorrectly.


## Version 2.17.0 {#2-17-0}

Released on December 4, 2024. To update to version **2.17.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* **_(Requires server v25.1+)_** **_(Experimental)_** Added the `{{ ydb-cli }} debug ping` command for performance and connectivity debugging.

### Performance

* Improved performance of parallel [importing data from the file system](./reference/ydb-cli/export-import/tools-restore.md) using the `{{ ydb-cli }} tools restore` command.

### Bug fixes

* Fixed a bug in the table schema created by the `{{ ydb-cli }} workload tpch` command where the `partsupp` table contained an incorrect list of key columns.
* Resolved an issue where the `{{ ydb-cli }} tools restore` command failed with the error "Too much data" if the maximum value of the `--upload-batchbytes` option was set to 16 MB.

## Version 2.16.0 {#2-16-0}

Released on November 26, 2024. To update to version **2.16.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Improved throughput of the `{{ ydb-cli }} import file csv` command by up to 3 times.
* Added support for running the [stock benchmark](./reference/ydb-cli/commands/workload/stock.md) with [column-oriented tables](./concepts/datamodel/table.md#column-oriented-tables).
* Added support for [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)–formatted timestamps in the `{{ ydb-cli }} topic` commands.
* Added the `--explain-ast` option to the `{{ ydb-cli }} sql` command, which prints the query AST.
* Added ANSI SQL syntax highlighting in interactive mode.
* Added support for [PostgreSQL syntax](./postgresql/intro.md) in the `{{ ydb-cli }} workload tpch` and `{{ ydb-cli }} workload tpcds` benchmarks.
* Introduced the `-c` option for the `{{ ydb-cli }} workload tpcds run` command to compare results with expected values and display differences.
* Added log events for the `{{ ydb-cli }} tools dump` and `{{ ydb-cli }} tools restore` commands.
* Enhanced the `{{ ydb-cli }} tools restore` command to display error locations.

### Backward incompatible changes

* Changed the default value of the `{{ ydb-cli }} topic write` command's `--codec` option to `RAW`.

### Bug fixes

* Fixed the progress bar in the `{{ ydb-cli }} workload import` command.
* Resolved an issue where restoring from a backup using the `--import-data` option could fail if the table's partitioning had changed.

## Version 2.10.0 {#2-10-0}

Released on June 24, 2024. To update to version **2.10.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added the `{{ ydb-cli }} sql` command that runs over QueryService and can execute any DML/DDL command.
* Added `notx` support for the `--tx-mode` option in the `{{ ydb-cli }} table query execute` command.
* Added start and end times for long-running operation descriptions (export, import).
* Added replication description support in the `{{ ydb-cli }} scheme describe` and `{{ ydb-cli }} scheme ls` commands.
* Added big datetime types support: `Date32`, `Datetime64`, `Timestamp64`, `Interval64`.
* `{{ ydb-cli }} workload` commands rework:

  * Added the `--clear` option to the `init` subcommand, allowing tables from previous runs to be removed before workload initialization.
  * Added the `{{ ydb-cli }} workload * import` command to prepopulate tables with initial content before executing benchmarks.

### Backward incompatible changes

* `{{ ydb-cli }} workload` commands rework:

  * The `--path` option was moved to a specific workload level. For example: `{{ ydb-cli }} workload tpch --path some/tables/path init ...`.
  * The `--store=s3` option was changed to `--store=external-s3` in the `init` subcommand.


### Bug fixes

* Fixed colors in the `PrettyTable` format

## Version 2.9.0 {#2-9-0}

Released on April 25, 2024. To update to version **2.9.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Improved query logical plan tables: added colors, more information, fixed some bugs.
* The verbose option `-v` is supported for the `{{ ydb-cli }} workload` commands to provide debug information.
* Added an option to run the `{{ ydb-cli }} workload tpch` command with an S3 source to measure [federated queries](concepts/federated_query/index.md) performance.
* Added the `--rate` option for `{{ ydb-cli }} workload` commands to control the transactions (or requests) per second limit.
* Added the `--use-virtual-addressing` option for S3 import/export, allowing the switch to [virtual hosting of buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html) for the S3 path layout.
* Improved the `{{ ydb-cli }} scheme ls` command performance due to listing directories in parallel.

### Bug fixes

* Resolved an issue where extra characters were truncated during line transfers in CLI tables.
* Fixed invalid memory access in `tools restore`.
* Fixed the issue of the `--timeout` option being ignored in generic and scan queries, as well as in the import command.
* Added a 60-second timeout to version checks and CLI binary downloads to prevent infinite waiting.
* Minor bug fixes.

## Version 2.8.0 {#2-8-0}

Released on January 12, 2024. To update to version **2.8.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added new `{{ ydb-cli }} admin config` and `{{ ydb-cli }} admin volatile-config` commands for cluster configuration management.
* Added support for loading PostgreSQL-compatible data types by [ydb import file csv|tsv|json](reference/ydb-cli/export-import/import-file.md) command. Only for row-oriented tables.
* Added support for directory load from an S3-compatible storage in the [ydb import s3](reference/ydb-cli/export-import/import-s3.md) command. Currently only available on Linux and Mac OS.
* Added support for outputting the results of [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb yql](reference/ydb-cli/yql.md) and [ydb scripting yql](reference/ydb-cli/scripting-yql.md) commands in the [Apache Parquet](https://parquet.apache.org/docs/) format.
* In the [ydb workload](reference/ydb-cli/commands/workload/index.md) commands, the `--executer` option has been added, which allows to specify which type of queries to use.
* Added a column with median benchmark execution time in the statistics table of the [ydb workload clickbench](reference/ydb-cli/workload-click-bench.md) command.
* **_(Experimental)_** Added the `generic` request type to the [ydb table query execute](reference/ydb-cli/table-query-execute.md) command, allowing to perform [DDL](https://en.wikipedia.org/wiki/Data_Definition_Language) and [DML](https://en.wikipedia.org/wiki/Data_Manipulation_Language) operations, return with arbitrarily-sized results and support for [MVCC](concepts/mvcc.md). The command uses an experimental API, compatibility is not guaranteed.
* **_(Experimental)_** In the `{{ ydb-cli }} table query explain` command, the `--collect-diagnostics` option has been added to collect query diagnostics and save it to a file. The command uses an experimental API, compatibility is not guaranteed.

### Bug fixes

* Fixed an error displaying tables in `pretty` format with [Unicode](https://en.wikipedia.org/wiki/Unicode) characters.

* Fixed an error substituting the wrong primary key in the command [ydb tools pg-convert](postgresql/import.md#pg-convert).

## Version 2.7.0 {#2-7-0}

Released on October 23, 2023. To update to version **2.7.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added the [ydb tools pg-convert](postgresql/import.md#pg-convert) command, which prepares a dump obtained by the [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) utility for loading into the YDB postgres-compatible layer.
* Added the `{{ ydb-cli }} workload query` load testing command, which loads the database with [script execution queries](reference/ydb-cli/yql.md) in multiple threads.
* Added new `{{ ydb-cli }} scheme permissions list` command to list permissions.
* In the commands [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb table query explain](reference/ydb-cli/commands/explain-plan.md), [ydb yql](reference/ydb-cli/yql.md), and [ydb scripting yql](reference/ydb-cli/scripting-yql.md), the `--flame-graph` option has been added, specifying the path to the file in which you need to save the visualization of query execution statistics.
* [Special commands](reference/ydb-cli/interactive-cli.md#spec-commands) in the interactive query execution mode are now case-insensitive.
* Added validation for [special commands](reference/ydb-cli/interactive-cli.md#spec-commands) and their [parameters](reference/ydb-cli/interactive-cli.md#internal-vars).
* Added table reading in the scenario with transactions in the command [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run).
* Added the `--commit-messages` option to the command [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run), specifying the number of messages in a single transaction.
* Added the options `--only-table-in-tx` and `--only-topic-in-tx` in the command [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run), specifying restrictions on the types of queries in a single transaction.
* Added new columns `Select time` and `Upsert time` in the statistics table in the command [ydb workload transfer topic-to-table run](reference/ydb-cli/workload-transfer.md#run).

### Bug fixes

* Fixed an error when loading an empty JSON list by commands: [ydb table query execute](reference/ydb-cli/table-query-execute.md), [ydb scripting yql](reference/ydb-cli/scripting-yql.md) and [ydb yql](reference/ydb-cli/yql.md).

## Version 2.6.0 {#2-6-0}

Released on September 7, 2023. To update to version **2.6.0**, select the [Downloads](downloads/ydb-cli.md) section.

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

## Version 2.5.0 {#2-5-0}

Released on June 20, 2023. To update to version **2.5.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* For the `{{ ydb-cli }} import file` command, a parameter [--timeout](reference/ydb-cli/export-import/import-file.md#optional) has been added that specifies the time within which the operation should be performed on the server.
* Added a progress bar in commands [ydb scheme rmdir --recursive](reference/ydb-cli/commands/dir.md#rmdir) and [ydb import file](reference/ydb-cli/export-import/import-file.md).
* Added the command [ydb workload kv run read-rows](reference/ydb-cli/workload-kv.md#read-rows-kv), which loads the database with requests to read rows using a new experimental API call ReadRows (implemented only in the [main](https://github.com/ydb-platform/ydb) branch), which performs faster key reading than [select](reference/ydb-cli/workload-kv.md#select-kv).
* New parameters `--warmup-time`, `--percentile`, `--topic` have been added to the [ydb workload topic](reference/ydb-cli/workload-topic.md), setting the test warm-up time, the percentile in the statistics output and the topic name, respectively.
* Added the [ydb workload tpch](reference/ydb-cli/workload-tpch.md) command to run the TPC-H benchmark.
* Added the `--ordered` flag in the command [ydb tools dump](reference/ydb-cli/export-import/tools-dump.md), which preserves the order by primary key in tables.

### Performance

* The data loading speed in the `{{ ydb-cli }} import file` command has been increased by adding parallel loading. The number of threads is set by the new parameter [--threads](reference/ydb-cli/export-import/import-file.md#optional).
* A performance of the [ydb import file json](reference/ydb-cli/export-import/import-file.md) command has been increased by reducing the number of data copies.

## Version 2.4.0 {#2-4-0}

Released on May 24, 2023. To update to version **2.4.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added the ability to upload multiple files in parallel with the command [ydb import file](reference/ydb-cli/export-import/import-file.md#multiple-files).
* Added support for deleting column tables for the command [ydb scheme rmdir --recursive](reference/ydb-cli/commands/dir.md#rmdir).
* Improved stability of the command [ydb workload topic](reference/ydb-cli/workload-topic.md).

## Version 2.3.0 {#2-3-0}

Release date: May 1, 2023. To update to version **2.3.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added the interactive mode of query execution. To switch to the interactive mode, run [ydb yql](reference/ydb-cli/yql.md) without arguments. This mode is experimental: backward compatibility is not guaranteed yet.
* Added the [ydb index rename](reference/ydb-cli/commands/secondary_index.md#rename) command for [atomic replacement](dev/secondary-indexes.md#atomic-index-replacement) or renaming of a secondary index.
* Added the `{{ ydb-cli }} workload topic` command for generating the load that reads messages from topics and writes messages to topics.
* Added the [--recursive](reference/ydb-cli/commands/dir.md#rmdir-options) option for the `{{ ydb-cli }} scheme rmdir` command. Use it to delete a directory recursively, with all its content.
* Added support for the `topic` and `coordination node` types in the [ydb scheme describe](reference/ydb-cli/commands/scheme-describe.md) command.
* Added the [--commit](reference/ydb-cli/topic-read.md#osnovnye-opcionalnye-parametry) option for the `{{ ydb-cli }} topic consumer` command. Use it to commit messages you have read.
* Added the [--columns](reference/ydb-cli/export-import/import-file.md#optional) option for the `{{ ydb-cli }} import file csv|tsv` command. Use it as an alternative to the file header when specifying a column list.
* Added the [--newline-delimited](reference/ydb-cli/export-import/import-file.md#optional) option for the `{{ ydb-cli }} import file csv|tsv` command. Use it to make sure that your data is newline-free. This option streamlines import by reading data from several file sections in parallel.

### Bug fixes

* Fixed the bug that resulted in excessive memory and CPU utilization when executing the `{{ ydb-cli }} import file` command.

## Version 2.2.0 {#2-2-0}

Release date: March 3, 2023. To update to version **2.2.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Fixed the error that didn't allow specifying supported compression algorithms when adding a topic consumer.
* Added support for streaming YQL scripts and queries based on options [transferred via `stdin`](reference/ydb-cli/parameterized-queries-cli.md).
* You can now [use a file](reference/ydb-cli/parameterized-queries-cli.md) to provide YQL query options
* Password input requests are now output to `stderr` instead of `stdout`.
* You can now save the root CA certificate path in a [profile](reference/ydb-cli/profile/index.md).
* Added a global option named [--profile-file](reference/ydb-cli/commands/global-options.md#service-options) to use the specified file as storage for profile settings.
* Added a new type of load testing: [ydb workload clickbench](reference/ydb-cli/workload-click-bench).

## Version 2.1.1 {#2-1-1}

Release date: December 30, 2022. To update to version **2.1.1**, select the [Downloads](downloads/ydb-cli.md) section.

### Improvements

* Added support for the `--stats` option of the [ydb scheme describe](reference/ydb-cli/commands/scheme-describe.md) command for column-oriented tables.
* Added support for Parquet files to enable their import with the [ydb import](reference/ydb-cli/export-import/import-file.md) command.
* Added support for additional logging and retries for the [ydb import](reference/ydb-cli/export-import/import-file.md) command.

## Version 2.1.0 {#2-1-0}

Release date: November 18, 2022. To update to version **2.1.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* You can now [create a profile non-interactively](reference/ydb-cli/profile/create.md#cmdline).
* Added the [ydb config profile update](reference/ydb-cli/profile/create.md#update) and [ydb config profile replace](reference/ydb-cli/profile/create.md#replace) commands to update and replace profiles, respectively.
* Added the `-1` option for the [ydb scheme ls](reference/ydb-cli/commands/scheme-ls.md) command to enable output of a single object per row.
* You can now save the IAM service URL in a profile.
* Added support for username and password-based authentication without specifying the password.
* Added support for AWS profiles in the [ydb export s3](reference/ydb-cli/export-import/auth-s3.md#auth) command.
* You can now create profiles using `stdin`. For example, you can pass the [YC CLI](https://yandex.cloud/docs/cli/) `yc ydb database get information` command output to the `{{ ydb-cli }} config profile create` command input.

### Bug fixes

* Fixed the error when request results were output in JSON-array format incorrectly if they included multiple server responses.
* Fixed the error that disabled profile updates so that an incorrect profile was used.

## Version 2.0.0 {#2-0-0}

Release date: September 20, 2022. To update to version **2.0.0**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added the ability to work with topics:

  * `{{ ydb-cli }} topic create`: Create a topic.
  * `{{ ydb-cli }} topic alter`: Update a topic.
  * `{{ ydb-cli }} topic write`: Write data to a topic.
  * `{{ ydb-cli }} topic read`: Read data from a topic.
  * `{{ ydb-cli }} topic drop`: Delete a topic.

* Added a new type of load testing:

  * `{{ ydb-cli }} workload kv init`: Create a table for kv load testing.
  * `{{ ydb-cli }} workload kv run`: Apply one of three types of load: run multiple `UPSERT` sessions, run multiple `INSERT` sessions, or run multiple sessions of GET requests by primary key.
  * `{{ ydb-cli }} workload kv clean`: Delete a test table.

* Added the ability to disable current active profile (see the `{{ ydb-cli }} config profile deactivate` command).
* Added the ability to delete a profile non-interactively with no commit (see the `--force` option under the `{{ ydb-cli }} config profile remove` command).
* Added CDC support for the `{{ ydb-cli }} scheme describe` command.
* Added the ability to view the current DB status (see the `{{ ydb-cli }} monitoring healthcheck` command).
* Added the ability to view authentication information (token) to be sent with DB queries under the current authentication settings (see the `{{ ydb-cli }} auth get-token` command).
* Added the ability for the `{{ ydb-cli }} import` command to read data from stdin.
* Added the ability to import data in JSON format from a file or stdin (see the `{{ ydb-cli }} import file json` command).

### Improvements

* Improved command processing. Improved the accuracy of user input parsing and validation.

## Version 1.9.1 {#1-9-1}

Release date: June 25, 2022. To update to version **1.9.1**, select the [Downloads](downloads/ydb-cli.md) section.

### Features

* Added the ability to compress data when exporting it to S3-compatible storage (see the `--compression` option of the [ydb export s3](reference/ydb-cli/export-import/export-s3.md) command).
* Added the ability to manage new {{ ydb-short-name }} CLI version availability auto checks (see the `--disable-checks` and `--enable-checks` options of the [ydb version](reference/ydb-cli/version.md) command).
