# Global parameters

## DB connection options {#connection-options}

DB connection options are described in [Connecting to and authenticating with a database](../../connect.md#command-line-pars).

## Service options {#service-options}

* `--profile <name>`: Indicates the use of the DB connection profile with the specified name when executing a {{ ydb-short-name }} CLI command. Most connection parameters can be stored in the profile.
* `-v, --verbose`: Prints detailed information about all operations being executed. Specifying this option is helpful when locating DB connection issues.
* `--profile-file`: Use profiles from the specified file. By default, profiles from the `~/.ydb/config/config.yaml` file are used.
