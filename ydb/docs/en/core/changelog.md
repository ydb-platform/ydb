# Releases

## 20.09.2022 {#20-09-2022}

{{ ydb-short-name }} CLI 2.0.0:

* Added `ydb topic` commands for operating with YDB topics:
  * `ydb topic create` — creates a topic with provided options;
  * `ydb topic alter` — changes topic configuration with provided options;
  * `ydb topic write` — writes data to provided topic;
  * `ydb topic read` — reads data from provided topic;
  * `ydb topic drop` drops provided topic.

* Added a new load testing type `ydb workload kv`:
  * `ydb workload kv init` — creates a table for kv load testing;
  * `ydb workload kv run` — starts one of 3 load types: launches several sessions performing insertion using `UPSERT`, launches several sessions performing insertion using `INSERT` or launches several sessions that create GET-requests by primary key;
  * `ydb workload kv clean` — drops the table created with init command.

* Added `ydb config profile deactivate` command to deactivate current active profile.
* Added `--force` option for `ydb config profile remove` command to remove profile without interactive confirmation.
* Added changefeed support for `ydb scheme describe` command.
* Added `ydb monitoring healthcheck` command to check current state of a database.
* Added `ydb auth get-token` command to print auth info (token) that would be sent with requests to YDB with current auth settings.
* `ydb import` commands can now read data from std input (i.e. via pipe) instead of a file.
* Added `ydb import file json` command to import data in JSON format to YDB from file or std input.
* Improved command processing. User input parsing and validation is now more precise.

## 25.06.2022 {#25-06-2022}

{{ ydb-short-name }} CLI 1.9.1:

* Added the ability to compress data when exporting it to S3-compatible storage (see the `--compression` parameter of the [ydb export s3](reference/ydb-cli/export_import/s3_export.md) command).
* Added the ability to manage new {{ ydb-short-name }} CLI version availability auto checks (see the `--disable-checks` and `--enable-checks` parameters of the [ydb version](reference/ydb-cli/version.md) command).