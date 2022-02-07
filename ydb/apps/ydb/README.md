# Yandex Database Command Line Interface

This is the official YDB command line client that works via public API.

The structure of commands in YDB CLI is similar to one in YDB API and consists of services (table, scheme, etc.).
There is also an additional "tools" section which provides useful utilities for common operations with the database.

There is a couple of options that every call has to be provided with (or it can be once set with "ydb init" command and saved to config):
- endpoint - Endpoint to connect to;
- database - Database to work with.

You can use --help option on root or any subcommand for more info.