# Аутентификация

В Open Source-версии {{ ydb-short-name }} работа с топиками по SQS-протоколу поддерживается только без аутентификации.

## Настройка кластера

Чтобы выключить аутентификацию, в разделе `security_config` [файла конфигурации кластера](../configuration/security_config.md#security-auth) установите параметр `enforce_user_token_requirement` в значение `false`:

```yaml
security_config:
  enforce_user_token_requirement: false
```
