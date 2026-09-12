# Connection and authentication

{{ ydb-short-name }} DSTool (`ydb-dstool`) talks to the cluster over two independent channels:

- **gRPC** — Blob Storage Controller (BSC) and other service APIs: reading configuration, changing PDisk status, VDisk and group operations. This is the primary interface for data retrieval and management.
- **HTTP** — requests to [{{ ydb-ui-name }}](../ydb-ui/index.md) (Viewer) and node monitoring. For example, some commands must check the current state of nodes and disks through the Viewer JSON API.

The endpoint and authentication method determine which channel can connect and which identity the server uses to authorize the request. This page describes how the utility selects a protocol and host, how [anonymous authentication](../../security/authentication.md#anonymous) works, and how to use [token authentication](#credentials).

For the full list of connection flags, see [{#T}](global-options.md).

## Endpoints {#endpoints}

Set the endpoint with the global `-e` / `--endpoint` option in the `[PROTOCOL://]HOST[:PORT]` format. You can pass the option more than once, including with different protocols.

Supported protocols:

| Protocol | Channel | Default port | Encryption |
|---|---|---|---|
| `grpc` | gRPC | `2135` | none |
| `grpcs` | gRPC | `2135` | TLS |
| `http` | HTTP Viewer / monitoring | `8765` | none |
| `https` | HTTP Viewer / monitoring | `8765` | TLS |

If you omit the protocol, the utility treats the endpoint as an HTTP Viewer address. If you omit the port, `grpc`/`grpcs` use `--grpc-port` (default `2135`) and `http`/`https` use `--mon-port` (default `8765`).

Examples:

```bash
# HTTP Viewer only (local cluster without TLS)
ydb-dstool -e http://localhost:8765 cluster list

# gRPC only. Required HTTP requests are sent to http://<host>:8765
ydb-dstool -e grpc://localhost:2135 cluster list

# Recommended for a cluster with authentication and TLS:
# both channels are specified explicitly
ydb-dstool \
  -e grpcs://static-node-1.example.com:2135 \
  -e https://static-node-1.example.com:8765 \
  --ca-file /path/to/ca.crt \
  --token-file /path/to/ydb-token \
  cluster list
```

For `grpcs` and `https`, pass the cluster root certificate in `--ca-file`. The `--insecure` flag disables certificate and hostname verification for HTTPS only; it does not affect gRPC.

## Protocol and host selection {#host-selection}

Each internal request is classified as HTTP, gRPC, or “either” (a BSC command may use gRPC or HTTP depending on the selected endpoint protocol).

The utility picks an address in the following order:

1. It takes endpoints of the required type from the `-e` list. If there are several, it picks a random host.
2. On a connection error it retries other endpoints of the same type (up to five attempts). A host that returns an HTTP error is marked bad for the rest of the run.
3. If there are no endpoints of the required type, the utility converts the specified addresses to endpoints of the other type:
   - an HTTP request from `grpc`/`grpcs://HOST:PORT` becomes `{http|https}://HOST:<mon-port>`;
   - a gRPC request from `http`/`https://HOST:PORT` becomes `{grpc|grpcs}://HOST:<grpc-port>`.
4. The conversion protocol is:
   - `https` if at least one `-e` value is `https` and none is `http`; otherwise `http`;
   - `grpcs` if at least one `-e` value is `grpcs` and none is `grpc`; otherwise `grpc`.

If you pass only `grpcs://...:2135`, the utility warns that no HTTP endpoint is set and sends HTTP requests to `http://<host>:8765`. On a cluster whose monitoring requires TLS this fails. Specify both endpoints to avoid conversion.

`--use-ip` resolves the hostname to an IP address before an HTTP request.

{% note warning %}

A final `Can't connect to specified addresses` after a series of `HTTP Error 403` messages is an access denial, not a network outage. Check the token format and the user's [access level](../configuration/security_config.md#security-access-levels).

{% endnote %}

## Anonymous authentication {#anonymous}

If the utility finds no token in any [source](#token-sources), it sends requests without credentials: HTTP without an `Authorization` header, and gRPC without `SecurityToken` or `x-ydb-auth-ticket` metadata.

This works on a local or test cluster that has [anonymous authentication](../../security/authentication.md#anonymous) enabled: [`enforce_user_token_requirement`](../configuration/security_config.md) is `false`, or is omitted entirely (the default).

To confirm that no token is picked up from the environment, omit `--token-file` and `--iam-token-file`, unset `YDB_TOKEN` and `IAM_TOKEN`, and make sure `~/.ydb/token` and `~/.ydb/iam_token` are absent. Then run a command such as `ydb-dstool -e http://localhost:8765 cluster list`.

{% note warning %}

Anonymous access is intended only for evaluation and local deployments. If the access-level lists in `security_config` are empty, any connecting client gets administrative privileges. Do not use anonymous authentication on clusters reachable over the network.

{% endnote %}

If the cluster requires authentication (`enforce_user_token_requirement: true`), an anonymous request is rejected. HTTP Viewer typically responds with `401 Unauthorized` (no `Authorization` header) or `403 Forbidden` (a header is present but the token is not accepted).

## Token authentication {#credentials}

{{ ydb-short-name }} DSTool does not accept a username and password on the command line and does not call the login service itself. The utility sends a ready-made (previously obtained) [authentication token](../../concepts/glossary.md#auth-token) issued with [{{ ydb-short-name }} CLI](../ydb-cli/index.md) (`auth get-token`).

To obtain the token, use the regular [authentication](../../security/authentication.md) flow: {{ ydb-short-name }} CLI sends credentials to the `Login` service, the server returns a token, and DSTool then attaches that token to every request.

### Getting a token {#get-token}

```bash
{{ ydb-cli }} --ca-file /path/to/ca.crt \
  -e grpcs://static-node-1.example.com:2135 \
  -d /Root \
  --user <user> \
  auth get-token --force > /tmp/ydb-login.jwt
```

If you do not pass `--password-file` or `--no-password`, the CLI prompts for the password. For the `root` user with an empty password during initial deployment, add `--no-password`.

### Token file format {#token-file-format}

`--token-file` reads the **first line** of the file. A single word is treated as an `OAuth` token. Two words are treated as a scheme and a token.

For a login token, set the scheme to `Login`. Otherwise HTTP Viewer receives `Authorization: OAuth <token>` and rejects the request (`403 Forbidden`), while gRPC BSC commands with the same file may still succeed: over gRPC the utility sends only the token body, without a scheme.

```bash
{ printf 'Login '; cat /tmp/ydb-login.jwt; } > /path/to/ydb-token
```

Example file contents:

```text
Login eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
```

A token that ends with `@builtin` (for example `root@builtin`) is sent without a scheme.

### How the token is sent {#token-transport}

| Channel | Where the token goes |
|---|---|
| HTTP Viewer | `Authorization: <scheme> <token>` header |
| gRPC BSC / CMS | `SecurityToken` field (token body only) |
| gRPC Distributed Storage and Bridge | `x-ydb-auth-ticket` metadata (token body only) |

### Token sources {#token-sources}

The utility uses the **first** source it finds:

1. `--token-file` — default scheme `OAuth` unless the file specifies one.
2. `--iam-token-file` — scheme `Bearer`. Mutually exclusive with `--token-file`.
3. `YDB_TOKEN` environment variable — scheme `OAuth` unless specified.
4. `IAM_TOKEN` environment variable — scheme `Bearer`.
5. `~/.ydb/token` — scheme `OAuth`.
6. `~/.ydb/iam_token` — scheme `Bearer`.

For login and password authentication, use `--token-file` with the `Login` scheme, or put the same line in `YDB_TOKEN` or `~/.ydb/token`.

### User privileges {#access-levels}

A successful login is not enough: the user's [SID](../../concepts/glossary.md#access-sid) must appear in the [`security_config`](../configuration/security_config.md#security-access-levels) access-level lists.

- Commands that change storage configuration through BSC require the **administration** level (`administration_allowed_sids`).
- HTTP Viewer requests, including the PDisk state check, require at least the **viewer** level (`viewer_allowed_sids`). A higher level includes the lower ones: administration grants monitoring and viewer.

It is usually enough to add a cluster administrator only to `administration_allowed_sids` (for example `root` or the `ADMINS` group). You can check the effective SID with [`{{ ydb-cli }} discovery whoami`](../ydb-cli/commands/discovery-whoami.md).

## Examples {#examples}

Anonymous access to a local cluster:

```bash
ydb-dstool -e http://localhost:8765 cluster list
```

A TLS cluster with login and password authentication:

```bash
{{ ydb-cli }} --ca-file /path/to/ca.crt \
  -e grpcs://static-node-1.example.com:2135 \
  -d /Root --user root \
  auth get-token --force > /tmp/ydb-login.jwt

{ printf 'Login '; cat /tmp/ydb-login.jwt; } > ~/ydb-token

ydb-dstool \
  -e grpcs://static-node-1.example.com:2135 \
  -e https://static-node-1.example.com:8765 \
  --ca-file /path/to/ca.crt \
  --token-file ~/ydb-token \
  pdisk set --status BROKEN --pdisk-ids '[9:1008]'
```
