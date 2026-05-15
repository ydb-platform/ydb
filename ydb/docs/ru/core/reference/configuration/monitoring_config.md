# monitoring_config

Секция `monitoring_config` файла конфигурации {{ ydb-short-name }} задаёт параметры [YDB Monitoring](../embedded-ui/ydb-monitoring.md).

## Аутентификация на страницах мониторинга {#authentication}

В этом разделе описаны настройки, связанные с [аутентификацией](../../security/authentication.md) на отдельных страницах встроенного мониторинга.

#|
|| Параметр | Описание ||
|| `require_counters_authentication` | Режим обязательной [аутентификации](../../security/authentication.md) на страницах `/counters` и `/counters/hosts`.

Возможные значения:

- `true` — доступ к `/counters` и `/counters/hosts` только с [аутентификационным токеном](../../concepts/glossary.md#auth-token); запросы проходят аутентификацию и проверку прав.

    Значение `true` допустимо только при включенном режиме обязательной [аутентификации](../../security/authentication.md) в секции [security_config](./security_config.md) файла конфигурации {{ ydb-short-name }}.

- `false` — запросы к `/counters` и `/counters/hosts` могут выполняться без [аутентификационного токена](../../concepts/glossary.md#auth-token).

Значение по умолчанию: `false`.
    ||
|| `require_healthcheck_authentication` | Дополнительное требование [аутентификации](../../security/authentication.md) для эндпоинта `/healthcheck` поверх общих правил кластера.

Возможные значения:

- `true` — любой ответ `/healthcheck`, включая [формат Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/) (параметр `format=prometheus`), выдаётся только при запросе с [аутентификационным токеном](../../concepts/glossary.md#auth-token); запросы проходят аутентификацию и проверку прав.

    Значение `true` допустимо только при включенном режиме обязательной [аутентификации](../../security/authentication.md) в секции [security_config](./security_config.md) файла конфигурации {{ ydb-short-name }}.

- `false` — при обязательной аутентификации в кластере запросы к `/healthcheck` без токена по-прежнему допускаются, если запрошен вывод в [формате Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/) (`format=prometheus`). Для остальных форматов ответа `/healthcheck` действуют общие правила (см. примечание ниже).

Значение по умолчанию: `false`.

{% note info %}

Если в [security_config](./security_config.md) включена обязательная [аутентификация](../../security/authentication.md), то для ответов `/healthcheck` в любом формате, кроме Prometheus, токен обязателен независимо от значения `require_healthcheck_authentication`.

{% endnote %}

||
|#

Примеры задания параметров, включающие  [аутентификацию](../../security/authentication.md) на отдельных страницах встроенного мониторинга.

```yaml
monitoring_config:
  # аутентификация на страницах /counters и /healthcheck
  require_counters_authentication: true
  require_healthcheck_authentication: true
```

## TLS на страницах мониторинга {#tls}

{{ ydb-short-name }} открывает отдельный HTTP-порт для работы [встроенного интерфейса](../../reference/embedded-ui/index.md), отображения [метрик](../../devops/observability/monitoring.md) и других вспомогательных команд.

На HTTP-порту можно включить TLS, что превращает его в HTTPS. Ниже описаны параметры [TLS](https://ru.wikipedia.org/wiki/Transport_Layer_Security) для [шифрования данных при передаче по сети](../../security/encryption/data-in-transit.md) в {{ ydb-short-name }}.

#|
|| Параметр | Описание ||
||

`monitoring_certificate`

|

Параметр для передачи содержимого SSL-сертификата и приватного SSL-ключа напрямую в [формате PEM](https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail) без использования отдельных файлов.

При указании этого параметра встроенный интерфейс автоматически начинает обрабатывать запросы с использованием указанного SSL-сертификата. Если указан параметр `monitoring_certificate`, параметры `monitoring_certificate_file` и `monitoring_private_key_file` игнорируются.

Значение по умолчанию: пустая строка.

||
||

`monitoring_certificate_file`

|

Путь к файлу сертификата для доступа по SSL в [формате PEM](https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail). Файл может дополнительно содержать приватный SSL-ключ. Этот приватный ключ будет использоваться в случае, если не указан параметр `monitoring_private_key_file`.

При указании параметра `monitoring_certificate_file` встроенный интерфейс автоматически начинает обрабатывать запросы с использованием указанного SSL-сертификата.

Значение по умолчанию: пустая строка.

||
||

`monitoring_private_key_file`
|

Путь к файлу приватного SSL-ключа в [формате PEM](https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail). При указании этого параметра должен быть задан параметр `monitoring_certificate_file`. Если в файле, указанном в параметре `monitoring_certificate_file`, содержится приватный SSL-ключ, он будет проигнорирован, то есть приоритет в задании приватного ключа имеет параметр `monitoring_private_key_file`.

Значение по умолчанию: пустая строка.

||
||

`monitoring_ca_file`

|

Путь к файлу с корневым (CA) сертификатом для проверки клиентских сертификатов ([mutual TLS](https://en.wikipedia.org/wiki/Mutual_authentication)). Указание этого пути включает возможность взаимной аутентификацию по сертификату (mTLS), оставляя возможность всех двух видов аутентификации, [поддерживаемых в YDB](../../security/authentication.md).

Значение по умолчанию: пустая строка.

||
|#

Пример включения mTLS для cтраниц мониторинга.

```yaml
monitoring_config:
  monitoring_certificate_file: /path/to/cert.pem # для включения TLS
  monitoring_private_key_file: /path/to/key.pem
  monitoring_ca_file: /path/to/ca.pem # для включения mTLS
```
