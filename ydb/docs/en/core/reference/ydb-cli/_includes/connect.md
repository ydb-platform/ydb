# Connecting the CLI to and authenticating with a database

Most of the {{ ydb-short-name }} CLI commands relate to operations on a {{ ydb-short-name }} database and require establishing a connection to it to be executed.

The {{ ydb-short-name }} CLI uses the following sources to determine the database to connect to and the [authentication mode](../../../concepts/auth.md) to use with it (listed in descending priority):

1. The command line.
2. The profile set in the `--profile` command-line option.
3. Environment variables.
4. The activated profile.

For the {{ ydb-short-name }} CLI to try connecting to the database, these steps must result in the [endpoint](../../../concepts/connect.md#endpoint) and [database path](../../../concepts/connect.md#database).

If all the steps are completed, but the {{ ydb-short-name }} CLI did not determine the authentication mode, requests will be sent to the {{ ydb-short-name }} server without adding authentication data. This may let you successfully work with locally deployed {{ ydb-short-name }} clusters that require no authentication. For all databases available over the network, such requests will be rejected by the server with an authentication error returned.

To learn about potential situations where the {{ ydb-short-name }} CLI won't try to connect to the database, see the [Error messages](#errors) below.

## Command line parameters {#command-line-pars}

DB connection options in the command line are specified before defining the command and its parameters:

```bash
{{ ydb-cli }} <connection_options> <command> <command_options>
```

- `-e, --endpoint <endpoint>` is the [endpoint](../../../concepts/connect.md#endpoint), that is, the main connection parameter that allows finding a {{ ydb-short-name }} server on the network. If no port is specified, port 2135 is used. If no protocol is specified, gRPCs (with encryption) is used in {{ ydb-short-name }} CLI public builds.
- `-d, --database <database>` is the [database path](../../../concepts/connect.md#database).

{% include [auth/options.md](auth/options.md) %}

## Parameters from the profile set by the command-line option {#profile}

If a certain connection parameter is not specified in the command line when calling the {{ ydb-short-name }} CLI, it tries to determine it by the [profile](../profile/index.md) set in the `--profile` command-line option.

In the profile, you can define most of the variables that have counterparts in the [Command line parameters](#command-line-pars) section. Their values are processed in the same way as command line parameters.

## Parameters from environment variables {#env}

If you did not explicitly specify a profile or authentication parameters at the command line, the {{ ydb-short-name }} CLI attempts to determine the authentication mode and parameters from the {{ ydb-short-name }} CLI environment as follows:

{% include [env.md](auth/env.md) %}

## Parameters from the activated profile {#activated-profile}

If some connection parameter could not be determined in the previous steps, and you did not explicitly specify a profile at the command line with the `--profile` option, the {{ ydb-short-name }} CLI attempts to use the connection parameters from the [activated profile](../profile/activate.md).

## Error messages {#errors}

### Errors before attempting to establish a DB connection

If the CLI completed all the steps listed at the beginning of this article but failed to determine the [endpoint](../../../concepts/connect.md#endpoint), the command terminates with the error `Missing required option 'endpoint'`.

If the CLI completed all the steps listed at the beginning of this article but failed to determine the [database path](../../../concepts/connect.md#database), the command terminates with the error message `Missing required option 'database'`.

If the authentication mode is known, but the necessary additional parameters are not, the command is aborted and an error message describing the issue is returned:

- `(No such file or directory) util/system/file.cpp:857: can't open "<filepath>" with mode RdOnly|Seq (0x00000028)`: Couldn't open and read the file `<filepath>` specified in a parameter passing the file name and path.

## Additional parameters {#additional}

When using gRPCs (with encryption), you may need to [select a root certificate](../../../concepts/connect.md#tls-cert).

- `--ca-file <filepath>`: Root certificate PEM file for a TLS connection.

## Authentication {#whoami}

The {{ ydb-short-name }} CLI [`discovery whoami`](../commands/discovery-whoami.md) auxiliary command lets you check the account that you actually used to authenticate with the server.
