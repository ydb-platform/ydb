# admin cluster config replace

С помощью команды `admin cluster config replace` вы можете загрузить [динамическую конфигурацию](../../../../../maintenance/manual/dynamic-config.md) на кластер {{ ydb-short-name }}.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
ydb [global options...] admin cluster config replace [options...]
```

* `global options` — глобальные параметры.
* `options` — [параметры подкоманды](#options).

Посмотрите описание команды замены динамической конфигурации:

```bash
ydb admin cluster config replace --help
```

## Параметры подкоманды {#options}

Имя | Описание
---|---
`-f`, `--filename` | Путь к файлу, содержащему конфигурацию.
`--ignore-local-validation` | Игнорировать локальные проверки применимости конфигурации (по умолчанию: 0).

## Примеры {#examples}

Загрузите файл динамической конфигурации на кластер:

```bash
ydb admin cluster config replace --filename сonfig.yaml
```

Загрузите файл динамической конфигурации на кластер, игнорируя локальные проверки применимости:

```bash
ydb admin cluster config replace -f config.yaml --ignore-local-validation
```
