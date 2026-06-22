# Authentication

In the Open Source version of {{ ydb-short-name }}, working with topics via the SQS protocol is supported only without authentication.

## Cluster configuration

To disable authentication, set the `enforce_user_token_requirement` parameter to `false` in the `security_config` section of the [cluster configuration file](../configuration/security_config.md#security-auth):

```yaml
security_config:
  enforce_user_token_requirement: false
```
