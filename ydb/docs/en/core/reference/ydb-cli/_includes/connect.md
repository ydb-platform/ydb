# Connecting the CLI to a database and authentication

<!-- markdownlint-disable blanks-around-fences -->

Most {{ ydb-short-name }} CLI commands relate to operations on the {{ ydb-short-name }} database and require a connection to it to run.

{{ ydb-short-name }} CLI determines which database to connect to and which [authentication mode](../../../security/authentication.md) to use from the following sources (in descending order of priority):

1. Command line
2. Profile selected by the command-line option `--profile`
3. Environment variables
4. Activated profile

For {{ ydb-short-name }} CLI to attempt to connect to the database, the [endpoint](../../../concepts/connect.md#endpoint) and [database path](../../../concepts/connect.md#database) must be determined as a result of executing these steps.

If all steps are completed but {{ ydb-short-name }} CLI has not determined an authentication mode, requests will be sent to the {{ ydb-short-name }} server without adding authentication data. This may allow successful work with locally deployed {{ ydb-short-name }} clusters that do not require authentication. For all network-accessible databases, such requests will be rejected by the server with an authentication error.

For possible situations when {{ ydb-short-name }} CLI will not attempt to connect to the database, see the [Error messages](#errors) section below.

## Command-line parameters {#command-line-pars}

Database connection options on the command line are specified before the command and its parameters:


```bash
{{ ydb-cli }} <опции_соединения> <команда> <опции_команды>
```


### Database connection parameters {#connection}

- `-e, --endpoint <endpoint>` — [endpoint](../../../concepts/connect.md#endpoint) is the main connection parameter that allows you to find the {{ ydb-short-name }} server on the network. If the port is not specified, 2135 is used. If the protocol is not specified, then in public builds of {{ ydb-short-name }} CLI, gRPCs (with encryption) is used.
- `-d, --database <database>` — [database path](../../../concepts/connect.md#database).
- `--no-discovery` — skip the discovery stage, during which a list of addresses for connecting to the YDB cluster is requested. If this option is set, the connection will be made directly to the endpoint specified by the user (using the `-e` option).

### Authentication parameters {#authentication}

{% include [auth/options.md](auth/options.md) %}

### TLS connection parameters {#tls}

{% include [auth/options_client_cert.md](auth/options_client_cert.md) %}

## Parameters from the profile selected by the command-line option {#profile}

If any connection parameter is not specified on the command line when invoking {{ ydb-short-name }} CLI, the CLI attempts to determine it from the [profile](../profile/index.md) selected by the command-line option `--profile`.

The profile can define most variables similar to the options from the [command-line parameters](#command-line-pars) section. Their values are processed in the same way as command-line parameters.

## Parameters from environment variables {#env}

If a profile was not explicitly specified on the command line or it does not contain authentication parameters, then {{ ydb-short-name }} CLI attempts to determine the authentication mode and parameters from the {{ ydb-short-name }} CLI environment using the following algorithm:

{% include [env.md](auth/env.md) %}

## Parameters from the activated profile {#activated-profile}

If at the previous steps it was not possible to determine any connection parameter and the profile was not explicitly specified in the command line with the `--profile` option, then {{ ydb-short-name }} CLI tries to use connection parameters from the [activated profile](../profile/activate.md).

## Error messages {#errors}

### Errors before attempting to connect to the database

If all the steps described at the beginning of this article are completed, but the [endpoint](../../../concepts/connect.md#endpoint) could not be determined, the command execution will be interrupted with the message `Missing required option 'endpoint'`.

If all the steps described at the beginning of this article are completed, but the [database path](../../../concepts/connect.md#database) could not be determined, the command execution will be interrupted with the message `Missing required option 'database'`.

If the authentication mode was determined, but the required additional parameters could not be determined, the command execution will be interrupted with a message describing the problem:


```text
(No such file or directory) util/system/file.cpp:857:
can't open "<filepath>" with mode RdOnly|Seq (0x00000028)
```


— could not open for reading the file `<filepath>` specified in one of the parameters that takes a file name with a path.

## Authentication check {#whoami}

The service command {{ ydb-short-name }} CLI [`discovery whoami`](../commands/discovery-whoami.md) allows you to check which account you are actually authenticated under on the server.
