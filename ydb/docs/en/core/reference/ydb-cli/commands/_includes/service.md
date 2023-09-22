# Service commands

These commands have to do with the {{ ydb-short-name }} CLI client itself and do not involve establishing a DB connection. They can be expressed either as a parameter or as an option.

| Name | Description |
| --- | --- |
| `-?`, `-h`, `--help` | Output the {{ ydb-short-name }} CLI syntax help |
| `version` | Output the {{ ydb-short-name }} CLI version (for public builds) |
| `update` | Update the {{ ydb-short-name }} CLI to the latest version (for public builds) |
| `config info` | Displaying [connection parameters](../../connect.md) |
| `--license` | Show the license (for public builds) |
| `--credits` | Show third-party product licenses (for public builds) |

If it is not known whether the used {{ ydb-short-name }} CLI build is public, you can find out if a particular service command is supported through help output.
