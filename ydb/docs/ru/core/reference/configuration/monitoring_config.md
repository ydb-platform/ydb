# monitoring_config

В разделе `monitoring_config` файла конфигурации {{ ydb-short-name }} задаются параметры [YDB Monitoring](../embedded-ui/ydb-monitoring.md). В этой статье перечислены лишь некоторые возможности конфигурации.

```yaml
monitoring_config:
  # настройки требования аутентификации на отдельных страницах YDB Monitoring
  require_counters_authentication: false
  require_healthcheck_authentication: false
```

## Настройки требования аутентификации на отдельных страницах YDB Monitoring {#authentication}

#|
|| Параметр | Описание ||
|| `require_counters_authentication` | Режим обязательной [аутентификации](../../security/authentication.md) на страницах `/counters` и `/counters/hosts`.

Возможные значения:

- `true` — аутентификация на страницах `/counters` и `/counters/hosts` обязательна, запросы к ним обязаны сопровождаться [аутентификационным токеном](../../concepts/glossary.md#auth-token). Запросы проходят аутентификацию и проверку прав.

    Значение `true` допустимо только при включенном режиме обязательной [аутентификации](../../security/authentication.md) в разделе [security_config](./security_config.md) файла конфигурации YDB.

- `false` — аутентификация на страницах `/counters` и `/counters/hosts` опциональна, запросы к ним могут не сопровождаться [аутентификационным токеном](../../concepts/glossary.md#auth-token).

Значение по умолчанию: `false`.

||

|| `require_healthcheck_authentication` | Режим обязательной [аутентификации](../../security/authentication.md) на странице `/healthcheck`.

- `true` — аутентификация на странице `/healthcheck` обязательна, запросы к ней обязаны сопровождаться [аутентификационным токеном](../../concepts/glossary.md#auth-token). Запросы проходят аутентификацию и проверку прав.

    Значение `true` допустимо только при включенном режиме обязательной [аутентификации](../../security/authentication.md) в разделе [security_config](./security_config.md) файла конфигурации YDB.

- `false` — аутентификация на странице `/healthcheck` обязательна для всех форматов, кроме [формата Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/) (CGI параметр `format=prometheus`), для которого запросы могут не сопровождаться [аутентификационным токеном](../../concepts/glossary.md#auth-token).

Значение по умолчанию: `false`.

{% note info %}

Для форматов страницы `/healthcheck`, отличных от [формата Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/), аутентификация обязательна при включенном режиме обязательной [аутентификации](../../security/authentication.md) независимо от значения описываемого параметра `require_healthcheck_authentication`.

{% endnote %}


||
|#
