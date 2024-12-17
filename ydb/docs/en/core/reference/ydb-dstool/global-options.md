# Global options

All the {{ ydb-short-name }} DSTool utility subcommands share the same global options.

| Option | Description |
---|---
| `-?`, `-h`, `--help` | Print the built-in help. |
| `-v`, `--verbose` | Print detailed output while executing the command. |
| `-q`, `--quiet` | Suppress non-critical messages when executing the command. |
| `-n`, `--dry-run` | Dry-run the command. |
| `-e`, `--endpoint` | Endpoint to connect to the {{ ydb-short-name }} cluster, in the format: `[PROTOCOL://]HOST[:PORT]`.<br/>Default values: PROTOCOL — `http`, PORT — `8765`. |
| `--grpc-port` | gRPC port used to invoke procedures. |
| `--mon-port` | Port to view HTTP monitoring data in JSON format. |
| `--mon-protocol` | If you fail to specify the cluster connection protocol explicitly in the endpoint, the protocol is taken from here. |
| `--token-file` | Path to the file with [Access Token](../../concepts/auth.md#iam). |
| `--ca-file` | Path to a root certificate PEM file used for TLS connections. |
| `--http` | Use HTTP instead of gRPC to connect to the Blob Storage. |
| `--http-timeout` | Timeout for I/O operations on the socket when running HTTP(S) queries. |
| `--insecure` | Allow insecure data delivery over HTTPS. Neither the SSL certificate nor host name are checked in this mode. |
