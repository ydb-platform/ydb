# Changelog

## 07/03/2023 {#07-03-2023}

### {{ ydb-short-name }} 22.5 {#ydb-22-5}

To update to version **22.5**, select the [Downloads](downloads/index.md) section.

**What's new:**

* Added [changefeed configuration parameters](yql/reference/syntax/alter_table#changefeed-options) to transfer additional information about changes to a topic.
* You can now [rename tables](concepts/datamodel/table.md#rename) that have TTL enabled.
* You can now [manage the record retention period](concepts/cdc#retention-period).

**Bug fixes and improvements:**

* Fixed an error inserting 0 rows with a BulkUpsert.
* Fixed an error importing Date/DateTime columns from CSV.
* Fixed an error importing CSV data with line breaks.
* Fixed an error importing CSV data with NULL values.
* Improved Query Processing performance (by replacing WorkerActor with SessionActor).
* DataShard compaction now starts immediately after a split or merge.

## 12/10/2022 {#12-10-2022}

**What's new in version 22.4**

To update to version **22.4**, select the [Downloads](downloads/index.md) section.

* {{ ydb-short-name }} Topics and Change Data Capture (CDC):
   * Introduced the new Topic API. {{ ydb-short-name }} [Topic](concepts/topic.md) is an entity for storing unstructured messages and delivering them to various subscribers.
   * Added support for the Topic API to the [{{ ydb-short-name }} CLI](reference/ydb-cli/topic-overview.md) and [SDK](reference/ydb-sdk/topic.md). The Topic API provides methods for message streaming writes and reads as well as topic management.
   * Added the ability to [capture table updates](concepts/cdc.md) and send change messages to a topic.

* SDK:
   * Added the ability to handle topics in the {{ ydb-short-name }} SDK.
   * Added official support for the database/sql driver for working with {{ ydb-short-name }} in Golang.

* Embedded UI:
   * The CDC change stream and the secondary indexes are now displayed in the database schema hierarchy as separate objects.
   * Improved the visualization of query explain plan graphics.
   * Problem storage groups have more visibility now.
   * Various improvements based on UX research.

* Query Processing:
   * Added Query Processor 2.0, a new subsystem to execute OLTP queries with significant improvements compared to the previous version.
   * Improved write performance by up to 60%, and by up to 10% for reads.
   * Added the ability to include a NOT NULL restriction for YDB primary keys when creating tables.
   * Added support for renaming a secondary index online without shutting the service down.
   * Improved the query explain view that now also includes fields for the physical operators.

* Core:
   * For read only transactions, added consistent snapshot support that does not conflict with write transactions.
   * Added BulkUpsert support for tables with asynchronous secondary indexes.
   * Added TTL support for tables with asynchronous secondary indexes.
   * Added compression support for data export to S3.
   * Added an audit log for DDL statements.
   * Added support for authentication with static credentials.
   * Added system tables for query performance troubleshooting.

## 20/09/2022 {#22-09-2022}

{{ ydb-short-name }} CLI 2.0.0:

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
* Added the ability to delete a profile non-interactively with no commit (see the `--force` parameter under the `ydb config profile remove` command).
* Added CDC support for the `ydb scheme describe` command.
* Added the ability to view the current DB status (see the `ydb monitoring healthcheck` command).
* Added the ability to view authentication information (token) to be sent with DB queries under the current authentication settings (see the `ydb auth get-token` command).
* Added the ability for the `ydb import` command to read data from stdin.
* Added the ability to import data in JSON format from a file or stdin (see the `ydb import file json` command).
* Improved command processing. Improved the accuracy of user input parsing and validation.

## 25/06/2022 {#25-06-2022}

{{ ydb-short-name }} CLI 1.9.1:

* Added the ability to compress data when exporting it to S3-compatible storage (see the `--compression` parameter of the [ydb export s3](reference/ydb-cli/export_import/s3_export.md) command).
* Added the ability to manage new {{ ydb-short-name }} CLI version availability auto checks (see the `--disable-checks` and `--enable-checks` parameters of the [ydb version](reference/ydb-cli/version.md) command).
