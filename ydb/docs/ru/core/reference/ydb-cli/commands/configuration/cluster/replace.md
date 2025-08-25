# admin cluster config replace

С помощью команды `admin cluster config replace` вы можете загрузить [конфигурацию](../../../../../devops/configuration-management/configuration-v2/index.md) на кластер {{ ydb-short-name }}.

{% include [danger-warning](../../_includes/danger-warning.md) %}

В зависимости от используемой кластером [версии конфигурации](../../../../../devops/configuration-management/compare-configs.md), команда заменяет:

* V1 — только [динамическую конфигурацию](../../../../../devops/configuration-management/configuration-v1/dynamic-config.md);
* V2 — всю конфигурацию.

Общий вид команды:

```bash
ydb [global options...] admin cluster config replace [options...]
```

* `global options` — глобальные параметры.
* `options` — [параметры подкоманды](#options).

Посмотрите описание команды замены конфигурации:

```bash
ydb admin cluster config replace --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `-f`, `--filename` | Путь к файлу, содержащему конфигурацию. ||
|| `--allow-unknown-fields`
| Разрешить наличие неизвестных полей в конфигурации.

Если флаг не указан, наличие неизвестных полей в конфигурации приводит к ошибке.
    ||
|| `--ignore-local-validation`
| Игнорировать базовую валидацию конфигурации на стороне клиента.

Если флаг не указан, YDB CLI проводит базовую валидацию конфигурации.
    ||
|#

## Примеры {#examples}

Загрузите файл конфигурации на кластер:

```bash
ydb admin cluster config replace --filename config.yaml
```

Загрузите файл конфигурации на кластер, игнорируя локальные проверки применимости:

```bash
ydb admin cluster config replace -f config.yaml --ignore-local-validation
```

Загрузите файл конфигурации на кластер, игнорируя проверку конфигурации на неизвестные поля:

```bash
ydb admin cluster config replace -f config.yaml --allow-unknown-fields
```
