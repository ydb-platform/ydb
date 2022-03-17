# Connecting the CLI to and authenticating with a database

Most of the {{ ydb-short-name }} CLI commands relate to operations on a {{ ydb-short-name }} database and require establishing a connection to it to be executed.

The {{ ydb-short-name }} CLI determines what DB to connect to and what [authentication mode](../../../concepts/connect.md#auth-modes) to use based on information from the following sources (in order of priority):

1. The command line.
2. The profile set in the `--profile` command-line option.
3. Environment variables.
4. The activated profile.

For the {{ ydb-short-name }} CLI to try connecting to the DB after you completed these steps, make sure to specify the [Endpoint](../../../concepts/connect.md#endpoint) and [Database location](../../../concepts/connect.md#database).

If all the steps are completed, but the {{ ydb-short-name }} CLI did not determine the authentication mode, requests will be sent to the {{ ydb-short-name }} server without adding authentication data. This may let you successfully work with locally deployed {{ ydb-short-name }} clusters that require no authentication. For all databases available over the network, such requests will be rejected by the server with an authentication error returned.

For possible situations when the {{ ydb-short-name }} CLI will not try to connect to a database, see the [Error messages](#errors) section below.

## Command line parameters {#command-line-pars}

DB connection options in the command line are specified before defining the command and its parameters:

```bash
{{ ydb-cli }} <connection_options> <command> <command_options>
```

* `-e, --endpoint <endpoint>` : [Endpoint](../../../concepts/connect.md#endpoint): The main connection parameter that allows finding the {{ ydb-short-name }} server on the network. If no port is specified, port 2135 is used. If no protocol is specified, gRPCs (with encryption) is used in {{ ydb-short-name }} CLI public builds.
* `-d, --database <database>` : [DB location](../../../concepts/connect.md#database).

{% include [auth/options.md](auth/options.md) %}

## Parameters from the profile set by the command-line option {#profile}

If a certain connection parameter is not specified in the command line when calling the {{ ydb-short-name }} CLI, it tries to determine it by the [profile](../profile/index.md) set in the `--profile` command-line option.

The profile may define most variables similar to the options from the [Command line parameters](#command-line-pars) section. Their values are processed in the same way as command line parameters.

## Parameters from environment variables {#env}

If you did not explicitly specify a profile or authentication parameters at the command line, the {{ ydb-short-name }} CLI attempts to determine the authentication mode and parameters from the {{ ydb-short-name }} CLI environment as follows:

{% include [env.md](auth/env.md) %}

## Parameters from the activated profile {#activated-profile}

If some connection parameter could not be determined in the previous steps, and you did not explicitly specify a profile at the command line with the `--profile` option, the {{ ydb-short-name }} CLI attempts to use the connection parameters from the [activated profile](../profile/activate.md).

## Error messages {#errors}

### Errors before attempting to establish a DB connection

If all the steps described in the beginning of this article are completed, but the [Endpoint](../../../concepts/connect.md#endpoint) is not determined, the command is aborted and an error message saying `Missing required option 'endpoint'` is returned.

If all the steps described in the beginning of this article are completed, but the [DB location](../../../concepts/connect.md#database) is not identified, the command is aborted and an error message saying `Missing required option 'database'` is returned.

If the authentication mode is known, but the necessary additional parameters are not, the command is aborted and an error message describing the issue is returned:

* `(No such file or directory) util/system/file.cpp:857: can't open "<filepath>" with mode RdOnly|Seq (0x00000028)`: Couldn't open and read the `<filepath>` file specified in one of the parameters with the file name and path.

## Additional parameters {#additional}

When using gRPCs (with encryption), you may need to [select a root certificate](../../../concepts/connect.md#tls-cert):

* `--ca-file <filepath>` : Root certificate PEM file for a TLS connection.

Currently, root certificates are not stored in profiles and can only be defined by command line options.

## Verifying authentication {#whoami}

The {{ ydb-short-name }} CLI [`discovery whoami`](../commands/discovery-whoami.md) auxiliary command lets you check the account that you actually used to authenticate with the server.
