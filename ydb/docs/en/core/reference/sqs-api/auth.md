# Authentication

In the open-source version of {{ ydb-short-name }}, working with topics via the Amazon SQS protocol is supported only without authentication.

## Cluster setup

To disable authentication, in the `security_config` section of the [cluster configuration file](../configuration/security_config.md#security-auth), set the `enforce_user_token_requirement` parameter to `false`:


```yaml
security_config:
  enforce_user_token_requirement: false
```
