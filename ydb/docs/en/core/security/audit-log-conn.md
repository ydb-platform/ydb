# Client connections audit

Client connections logging allows to monitor origin IP addresses and identities of the users who make client requests.

## Record attributes

| __Attribute__ | __Description__ |
|:----|:----|
| `component` | Logging component name, always `grpc-conn`. |
| `remote_address` | IP address of the client who sent the request. |
| `subject` | User SID (account name) of the user on whose behalf the operation is performed. |
| `database` | Path of the database in which the operation is performed. |
| `operation` | Request type. |

## How to enable

The audit logging must be [enabled](audit-log.md#enabling-audit-log) on the cluster level.

Client connections logging is enabled by the `feature_flags.enable_grpc_audit` flag through changing the [cluster configuration](../maintenance/manual/config-overview.md).

[//]: # (TODO: add link to a feature-flags section, not exist atm)

## Things to know

[//]: # (TODO: add a variable for links to the glossary)
- Logging occurs at the {{ ydb-short-name }} component `gRPC Proxy`.

- Interaction with the client takes place using the [gRPC](https://grpc.io/) protocol, which hides real network connections behind higher-level concepts. Therefore, instead of the fact of establishing a network connection, `gRPC Proxy` logs the fact of the arrival of a client request.

- The log only captures requests that have passed [authentication](../deploy/configuration/config#auth) and authorization checks against the database. When authentication is disabled, all requests are logged.

## Log record example

```json
{
    "component": "grpc-conn",
    "remote_address": "xxxx:xxx:xxxx:xx:xxxx:xxxx:xxxx:xxx",
    "subject": "bfbohb360qqqql1ef604@ad",
    "database": "/root/db"
    "operation": "ExecuteDataQueryRequest",
}
```
