# Releases

## 10.12.2022 {#10-12-2022}

**What's new in version 22.4**

To upgrade to **22.4** version see [Downloads](downloads/index.md).

* {{ ydb-short-name }} Topics and Change Data Capture (CDC):
  * A new Topic API is introduced in {{ ydb-short-name }}. {{ ydb-short-name }} [topic](concepts/topic.md) is an entity for storing unstructured messages and delivering them to multiple subscribers.
  * A new TopicAPI is supported in [{{ ydb-short-name }} CLI](reference/ydb-cli/topic-overview.md) and [SDK](reference/ydb-sdk/topic.md). The API enables topic management operations and streaming message publish/subscribe.
  * Added support of table data changes capture into a topic using [CDC change feeds](concepts/cdc.md).

* SDK:
  * {{ ydb-short-name }} Topics support added in YDB Golang SDK.
  * Official database/sql driver to work with YDB is now supported for Golang.

* Embedded UI:
  * CDC change feeds and Secondary Indexes are displayed in Database scheme hierarchy as separate objects.
  * Visualization of query explain plan was completely redesigned.
  * Storage groups with problems are now more visible.
  * Various optimizations based on UX assessment

* Query Processing:
  * Added Query Processor 2.0 — a brand new version of execution engine for OLTP-queries with significant improvements comparing to previous one.
  * Improved performance: up to 10% better read throughout, up to 60% better write throughput.
  * A feature for NOT NULL constraint for Primary Key has been added. Can be set up at table creation stage only.
  * Added support of online secondary index rename without stopping the service.
  * Improved query explain representation now includes graph for physical operators.

* Core:
  * Read-only transactions now use a consistent snapshot that doesn't conflict with concurrent writes.
  * Added BulkUpsert support for tables with async secondary indexes.
  * Added TTL support for tables with async secondary indexes.
  * Added option to compress data during export to S3.
  * Added initial version of audit log for DDL statements.
  * Added support of authentication with static credentials.
  * Added system tables for query performance diagnostics.

## 09.22.2022 {#22-09-2022}

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

## 06.25.2022 {#25-06-2022}

{{ ydb-short-name }} CLI 1.9.1:

* Added the ability to compress data when exporting it to S3-compatible storage (see the `--compression` parameter of the [ydb export s3](reference/ydb-cli/export_import/s3_export.md) command).
* Added the ability to manage new {{ ydb-short-name }} CLI version availability auto checks (see the `--disable-checks` and `--enable-checks` parameters of the [ydb version](reference/ydb-cli/version.md) command).
