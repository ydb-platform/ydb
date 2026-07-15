# YDB Command Line Interface

This is the official YDB command line client that works via public API.

Full documentation: https://ydb.tech/docs/en/reference/ydb-cli/

The structure of commands in YDB CLI is similar to one in YDB API and consists of services (table, scheme, etc.).
There is also an additional "tools" section which provides useful utilities for common operations with the database.

There is a couple of options that every call has to be provided with (or it can be once set with "ydb init" command and saved to config):
- endpoint - Endpoint to connect to;
- database - Database to work with.

You can use --help option on root or any subcommand for more info.

## Development

- App entrypoint: `ydb/apps/ydb/`; main library: `ydb/public/lib/ydb_cli/`.
- Build (release): `./ya make -r ydb/apps/ydb`.
- User-visible changes must be recorded in [CHANGELOG.md](CHANGELOG.md).

### Tests

Run with `./ya make -tA <dir>` (Linux only):

- Unit and binary tests (C++): `ut/` dirs next to the code, plus `ydb/apps/ydb/ut/` (drives the built `ydb` binary against a YDB recipe).
- Functional tests (Python): `ydb/tests/functional/ydb_cli/` (start a real cluster and run the `ydb` binary against it).