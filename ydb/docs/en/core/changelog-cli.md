# {{ ydb-short-name }} CLI changelog

## Version 2.3.0 {#2-3-0}

Release date: May 1, 2023. To update to version **2.3.0**, select the [Downloads](downloads/index.md#ydb-cli) section.

**What's new:**

* Added the interactive mode of query execution. To switch to the interactive mode, run [ydb yql](reference/ydb-cli/yql.md) without arguments. This mode is experimental: backward compatibility is not guaranteed yet.
* Added the [ydb index rename](reference/ydb-cli/commands/_includes/secondary_index.md#rename) command for [atomic replacement](best_practices/secondary_indexes.md#atomic-index-replacement) or renaming of a secondary index.
* Added the `ydb workload topic` command for generating the load that reads messages from topics and writes messages to topics.
* Added the [--recursive](reference/ydb-cli/commands/_includes/dir.md#rmdir-options) option for the `ydb scheme rmdir` command. Use it to delete a directory recursively, with all its content.
* Added support for the `topic` and `coordination node` types in the [ydb scheme describe](reference/ydb-cli/commands/scheme-describe.md) command.
* Added the [--commit](reference/ydb-cli/topic-read.md#osnovnye-opcionalnye-parametry) option for the `ydb topic consumer` command. Use it to commit messages you have read.
* Added the [--columns](reference/ydb-cli/export_import/_includes/import-file.md#optional) option for the `ydb import file csv|tsv` command. Use it as an alternative to the file header when specifying a column list.
* Added the [--newline-delimited](reference/ydb-cli/export_import/_includes/import-file.md#optional) option for the `ydb import file csv|tsv` command. Use it to make sure that your data is newline-free. This option streamlines import by reading data from several file sections in parallel.

**Bug fixes:**

* Fixed the bug that resulted in excessive memory and CPU utilization when executing the `ydb import file` command.

## Version 2.2.0 {#2-2-0}

Release date: March 3, 2023. To update to version **2.2.0**, select the [Downloads](downloads/index.md#ydb-cli) section.

**What's new:**

* Fixed the error that didn't allow specifying supported compression algorithms when adding a topic consumer.
* Added support for streaming YQL scripts and queries based on options [transferred via `stdin`](reference/ydb-cli/parameterized-queries-cli.md).
* You can now [use a file](reference/ydb-cli/parameterized-queries-cli.md) to provide YQL query options
* Password input requests are now output to `stderr` instead of `stdout`.
* You can now save the root CA certificate path in a [profile](reference/ydb-cli/profile/index.md).
* Added a global option named [--profile-file](reference/ydb-cli/commands/_includes/global-options.md#service-options) to use the specified file as storage for profile settings.
* Added a new type of load testing: [ydb workload clickbench](reference/ydb-cli/workload-click-bench).

## Version 2.1.1 {#2-1-1}

Release date: December 30, 2022. To update to version **2.1.1**, select the [Downloads](downloads/index.md#ydb-cli) section.

**Improvements:**

* Added support for the `--stats` option of the [ydb scheme describe](reference/ydb-cli/commands/scheme-describe.md) command for column-oriented tables.
* Added support for Parquet files to enable their import with the [ydb import](reference/ydb-cli/export_import/import-file.md) command.
* Added support for additional logging and retries for the [ydb import](reference/ydb-cli/export_import/import-file.md) command.

## Version 2.1.0 {#2-1-0}

Release date: November 18, 2022. To update to version **2.1.0**, select the [Downloads](downloads/index.md#ydb-cli) section.

**What's new:**

* You can now [create a profile non-interactively](reference/ydb-cli/profile/create.md#cmdline).
* Added the [ydb config profile update](reference/ydb-cli/profile/create.md#update) and [ydb config profile replace](reference/ydb-cli/profile/create.md#replace) commands to update and replace profiles, respectively.
* Added the `-1` option for the [ydb scheme ls](reference/ydb-cli/commands/scheme-ls.md) command to enable output of a single object per row.
* You can now save the IAM service URL in a profile.
* Added support for username and password-based authentication without specifying the password.
* Added support for AWS profiles in the [ydb export s3](reference/ydb-cli/export_import/s3_conn.md#auth) command.
* You can now create profiles using `stdin`. For example, you can pass the [YC CLI](https://cloud.yandex.ru/docs/cli/) `yc ydb database get information` command output to the `ydb config profile create` command input.

**Bug fixes:**

* Fixed the error when request results were output in JSON-array format incorrectly if they included multiple server responses.
* Fixed the error that disabled profile updates so that an incorrect profile was used.

## Version 2.0.0 {#2-0-0}

Release date: September 20, 2022. To update to version **2.0.0**, select the [Downloads](downloads/index.md#ydb-cli) section.

**What's new:**

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

**Improvements:**

* Improved command processing. Improved the accuracy of user input parsing and validation.

## Version 1.9.1 {#1-9-1}

Release date: June 25, 2022. To update to version **1.9.1**, select the [Downloads](downloads/index.md#ydb-cli) section.

**What's new:**

* Added the ability to compress data when exporting it to S3-compatible storage (see the `--compression` option of the [ydb export s3](reference/ydb-cli/export_import/s3_export.md) command).
* Added the ability to manage new {{ ydb-short-name }} CLI version availability auto checks (see the `--disable-checks` and `--enable-checks` options of the [ydb version](reference/ydb-cli/version.md) command).
