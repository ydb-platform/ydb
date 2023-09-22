# Просмотр параметров соединения

`config info` — это сервисная команда для отладки различных проблем с [соединением и аутентификацей](../connect.md). Команда выводит итоговые параметры соединения, вроде [эндпоинта](../../../concepts/connect.md#endpoint) и [пути базы данных](../../../concepts/connect.md#database), полученные рассмотрением параметров из всевозможных источников. С этими параметрами CLI подключилась бы при выполнении команды, подразумевающей соединение с базой данных. При указании [глобальной опции](global-options.md) `-v`, помимо итоговых параметров соединения, будут выведены значения всех параметров соединения из всех источников, которые CLI смог обнаружить, вместе с названиями этих источников, в порядке приоритета.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] config info
```

* `global options` — [глобальные параметры](global-options.md).

## Список параметров соединения {#parameters-list}

* [endpoint](../../../concepts/connect.md#endpoint) — URL кластера базы данных.
* [database](../../../concepts/connect.md#database) — Путь к базе данных.
* **Параметры аутентификации:**
  * [token](../../../concepts/auth.md#iam) — Access Token.
  * [yc-token](../../../concepts/auth.md#iam) — Refresh Token.
  * [sa-key-file](../../../concepts/auth.md#iam) — Service Account Key.
  * [use-metadata-credentials](../../../concepts/auth.md#iam) — Metadata.
  * [user](../../../concepts/auth.md#static-credentials)
  * [password](../../../concepts/auth.md#static-credentials)
* [ca-file](../../../concepts/connect.md#tls-cert) — Корневой сертификат.
* [iam-endpoint](../../../concepts/auth.md#iam) — URL IAM сервиса.

## Примеры {#examples}

### Вывод итоговых параметров соединения {#basic-example}

```bash
$ ydb -e grpcs://another.endpoint:2135 --ca-file some_certs.crt -p db123 config info
endpoint: another.endpoint:2135
yc-token: SOME_A12****************21_TOKEN
iam-endpoint: iam.api.cloud.yandex.net
ca-file: some_certs.crt
```

### Вывод всех параметров соединения вместе с источниками {#verbose-example}

```bash
$ ydb -e grpcs://another.endpoint:2135 --ca-file some_certs.crt -p db123 -v config info
Using Yandex.Cloud Passport token from YC_TOKEN env variable

endpoint: another.endpoint:2135
yc-token: SOME_A12****************21_TOKEN
iam-endpoint: iam.api.cloud.yandex.net
ca-file: some_certs.crt
current auth method: yc-token

"ca-file" sources:
  1. Value: some_certs.crt. Got from: explicit --ca-file option

"database" sources:
  1. Value: /some/path. Got from: active profile "test_config_info"

"endpoint" sources:
  1. Value: another.endpoint:2135. Got from: explicit --endpoint option
  2. Value: db123.endpoint:2135. Got from: profile "db123" from explicit --profile option
  3. Value: some.endpoint:2135. Got from: active profile "test_config_info"

"iam-endpoint" sources:
  1. Value: iam.api.cloud.yandex.net. Got from: default value

"sa-key-file" sources:
  1. Value: /Users/username/some-sa-key-file. Got from: active profile "test_config_info"

"yc-token" sources:
  1. Value: SOME_A12****************21_TOKEN. Got from: YC_TOKEN enviroment variable
```
