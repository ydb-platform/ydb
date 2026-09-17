# Connecting the CLI to a database and authentication

<!-- markdownlint-disable blanks-around-fences -->

Most {{ ydb-short-name }} CLI commands relate to operations on the {{ ydb-short-name }} database and require a connection to it to run.

{{ ydb-short-name }} CLI determines which database to connect to and which [authentication mode](../../../security/authentication.md) to use from the following sources (in descending order of priority):

1. Command line
2. Profile selected by the `--profile` command-line option
3. Environment variables
4. Activated profile

For {{ ydb-short-name }} CLI to attempt to connect to the database, the [endpoint](../../../concepts/connect.md#endpoint) and [database path](../../../concepts/connect.md#database) must be determined as a result of these steps.

If all steps are completed but {{ ydb-short-name }} CLI has not determined an authentication mode, requests will be sent to the {{ ydb-short-name }} server without authentication data. This may allow successful work with locally deployed {{ ydb-short-name }} clusters that do not require authentication. For all network-accessible databases, such requests will be rejected by the server with an authentication error.

For possible situations where {{ ydb-short-name }} CLI will not attempt to connect to the database, see the [Error messages](#errors) section below.

## Command-line parameters {#command-line-pars}

Database connection options on the command line are specified before the command and its parameters:


```bash
{{ ydb-cli }} <connection_options> <command> <command_options>
```


### Database connection parameters {#connection}

- `-e, --endpoint <endpoint>` — [endpoint](../../../concepts/connect.md#endpoint) — the main connection parameter that allows you to find the {{ ydb-short-name }} server on the network. If no port is specified, 2135 is used. If no protocol is specified, gRPCs (with encryption) is used in public builds of {{ ydb-short-name }} CLI.
- `-d, --database <database>` — [database path](../../../concepts/connect.md#database).
- `--no-discovery` — skip the discovery stage, which requests a list of addresses for connecting to the YDB cluster. If this option is set, the connection will be made directly to the endpoint specified by the user (using the `-e` option).

### Authentication parameters {#authentication}

{% include [auth/options.md](auth/options.md) %}

### TLS connection parameters {#tls}

{% include [auth/options_client_cert.md](auth/options_client_cert.md) %}

## Parameters from the profile selected by the command-line option {#profile}

If any connection parameter is not specified on the command line when invoking {{ ydb-short-name }} CLI, the CLI attempts to determine it from the [profile](../profile/index.md) selected by the `--profile` command-line option.

The profile can define most variables similar to the options in the [command-line parameters](#command-line-pars) section. Their values are processed in the same way as command-line parameters.

## Parameters from environment variables {#env}

If no profile was explicitly specified on the command line or it does not contain authentication parameters, {{ ydb-short-name }} CLI attempts to determine the authentication mode and parameters from the {{ ydb-short-name }} CLI environment using the following algorithm:

{% include [env.md](auth/env.md) %}

## Parameters from the activated profile {#activated-profile}

If on the previous steps it was not possible to determine any connection parameter and no profile was explicitly specified in the command line with the `--profile` option, then {{ ydb-short-name }} CLI tries to use connection parameters from the [activated profile](../profile/activate.md).

## Error messages {#errors}

### Errors before attempting to connect to the database

If all the steps described at the beginning of this article are completed, but the [endpoint](../../../concepts/connect.md#endpoint) could not be determined, the command will be aborted with the message `Missing required option 'endpoint'`.

If all the steps described at the beginning of this article are completed, but the [database path](../../../concepts/connect.md#database) could not be determined, the command will be aborted with the message `Missing required option 'database'`.

If the authentication mode was determined, but the required additional parameters could not be determined, the command will be aborted with a message describing the problem:

- `(No such file or directory) util/system/file.cpp:857: can't open "<filepath>" with mode RdOnly|Seq (0x00000028)` -- failed to open the file `<filepath>` for reading, specified in one of the parameters where the file name with path is passed.

## Authentication check {#whoami}

The service command {{ ydb-short-name }} CLI [`discovery whoami`](../commands/discovery-whoami.md) allows you to check which account you are actually authenticated as on the server.
