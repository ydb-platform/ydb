# http_proxy_config

В разделе `http_proxy_config` файла конфигурации {{ ydb-short-name }} включаются и конфигурируются AWS совместимые протоколы, которые дают доступ к работе с [{{ ydb-short-name }} Topics](../../concepts/datamodel/topic.md) по следующим протоколам:

* [SQS](../../reference/sqs-api/index.md).

## Описание параметров

#|
|| Параметр | Тип | Значение по умолчанию | Описание ||
|| `enabled` | bool | `false` | Включает или отключает Http Proxy. ||
|| `port` | int32 | | Порт, на котором будет доступен Http Proxy. ||
|| `secure` | bool | `false` | Включает использование TLS. ||
|| `ca` | string | - | Путь к файлу certificate authority для mTLS. ||
|| `cert` | string | - | Путь к файлу сертификата для доступа по TLS. При указании этого параметра Http Proxy начинает обрабатывать запросы с использованием указанного сертификата. ||
|| `key` | string | - | Путь к файлу ключа для доступа по TLS. ||
|| `sqs_topic_enabled` | bool | `false` | Включает поддержку протокола SQS для работы с топиками. ||
|| `yandex_cloud_service_region` | list\<string\> | `[]` | Список AWS-совместимых регионов, которые будет использовать SQS API. ||
|#

## Пример заполненного конфига

```yaml
http_proxy_config:
  enabled: true
  port: 8443
  secure: true
  ca: /path/to/CA.pem
  cert: /path/to/cert.pem
  key: /path/to/key.pem
  sqs_topic_enabled: true
  yandex_cloud_service_region:
    - "ydb"
```
