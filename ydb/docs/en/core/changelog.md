# Releases

## 20.09.2022 {#22-09-2022}

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

## 25.06.2022 {#25-06-2022}

{{ ydb-short-name }} CLI 1.9.1:

* Added the ability to compress data when exporting it to S3-compatible storage (see the `--compression` parameter of the [ydb export s3](reference/ydb-cli/export_import/s3_export.md) command).
* Added the ability to manage new {{ ydb-short-name }} CLI version availability auto checks (see the `--disable-checks` and `--enable-checks` parameters of the [ydb version](reference/ydb-cli/version.md) command).
