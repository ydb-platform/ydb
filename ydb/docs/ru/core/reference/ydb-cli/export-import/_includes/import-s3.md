# Загрузка из S3-совместимого хранилища

Команда `import s3` запускает на стороне сервера процесс загрузки из S3-совместимого хранилища данных и информации об объектах схемы, в описанном в статье [Файловая структура](../file-structure.md) формате:

```bash
{{ ydb-cli }} [connection options] import s3 [options]
```

{% note info %}

Импорт таблиц из S3-совместимого хранилища данных в других форматах возможен с использованием [внешних таблиц](../../../../concepts/query_execution/federated_query/s3/external_table.md), подробнее см. в статье [{#T}](../../../../concepts/query_execution/federated_query/import_and_export.md#import).

{% endnote %}

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

В отличие от [команды `tools restore`](../tools-restore.md), команда `import s3` всегда создает объекты целиком, поэтому для её успешного выполнения ни один из загружаемых объектов (ни директорий, ни таблиц) не должен существовать.

При необходимости догрузки данных в существующие таблицы из S3 вы можете скопировать содержимое S3 в файловую систему (например, с помощью [S3cmd](https://s3tools.org/s3cmd)) и воспользоваться [командой `tools restore`](../tools-restore.md).

## Параметры командной строки {#pars}

`[options]` - параметры команды:

### Параметры S3 {#s3-params}

Команда загрузки из S3 требует указания [параметров соединения с S3](../auth-s3.md). Так как загрузка производится в асинхронном режиме сервером {{ ydb-short-name }}, указанный эндпоинт должен быть доступен для установки соединения со стороны сервера.

`--source-prefix PREFIX`: Префикс загрузки в бакете S3.

### Загружаемые объекты схемы базы данных {#objects}

{% include [import-objects-params.md](./import-objects-params.md) %}

{% cut "Альтернативный способ" %}

{% include [import-alternative-syntax.md](./import-alternative-syntax.md) %}

- `source`, `src` или `s` — префикс ключа в S3 с загружаемой директорией или таблицей.
- `destination`, `dst`, или `d` —  путь в базе данных для размещения загружаемой директории или таблицы. Конечный элемент пути не должен существовать. Все директории на пути будут созданы, если не существуют.

{% include [import-alternative-syntax-warning.md](./import-alternative-syntax-warning.md) %}

{% endcut %}

### Дополнительные параметры {#aux}

| Параметр | Описание |
| --- | --- |
| `--description STRING` | Текстовое описание операции, сохраняемое в истории операций. |
| `--retries NUM` | Количество повторных попыток загрузки, которые будет предпринимать сервер.<br/>Значение по умолчанию: `10`. |
| `--index-population-mode STRING` | Режим наполнения индексов при импорте. Применяется, если выгрузка была выполнена с опцией [`--include-index-data`](../export-s3.md#aux) команды `export s3`.<br/>Допустимые значения:<br/><ul><li>`build` — построить индексы с нуля (значение по умолчанию);</li><li>`import` — скопировать данные индексных таблиц из выгрузки;</li><li>`auto` — попытаться скопировать данные индексных таблиц из выгрузки, при отсутствии данных построить индексы.</li></ul> |
| `--skip-checksum-validation` | Пропустить этап валидации [контрольных сумм](../file-structure.md#checksums) загружаемых объектов. |
| `--encryption-key-file PATH` | Путь к файлу, содержащему ключ шифрования (только для зашифрованных выгрузок). Данный файл является бинарным и должен содержать точное количество байт, соответствующее длине ключа в выбранном алгоритме шифрования (16 байт для `AES-128-GCM`, 32 байта для `AES-256-GCM` и `ChaCha20-Poly1305`). Ключ также может быть передан через переменную окружения `YDB_ENCRYPTION_KEY`, в шестнадцатеричном строковом представлении. |
| `--list` | Перечислить объекты в существующей выгрузке. |
| `--format STRING` | Формат вывода результата.<br/>Допустимые значения:<br/><ul><li>`pretty` — человекочитаемый формат (по умолчанию);</li><li>`proto-json-base64` — [Protocol Buffers](https://ru.wikipedia.org/wiki/Protocol_Buffers) в формате [JSON](https://ru.wikipedia.org/wiki/JSON), бинарные строки закодированы в [Base64](https://ru.wikipedia.org/wiki/Base64).</li></ul> |

{% note info %}

Режим `import` позволяет максимально задействовать ресурсы и быстро восстановить индексы, так как S3 оптимизирован под массовое чтение, либо наоборот — ограничить ресурсы и скопировать индексные таблицы с минимальным влиянием на пользовательскую нагрузку. Для ограничения ресурсов импорта используется [очередь `queue_restore`](../../../configuration/resource_broker_config.md) брокера ресурсов.

{% endnote %}

## Выполнение загрузки {#exec}

{% include [server-import-workflow.md](server-import-workflow.md) %}

### Результат запуска {#result}

При успешном исполнении команда `import s3` выводит сводную информацию о поставленной в очередь операции загрузки из S3, в заданном опцией `--format` формате. Фактическая загрузка производится сервером асинхронно. В сводной информации выводится ID операции, который может быть использован в дальнейшем для проверки статуса и действий с операцией:

{% include [import-operation-result-pretty-intro.md](import-operation-result-pretty-intro.md) %}

  ```text
  ┌───────────────────────────────────────────┬───────┬─────...
  | id                                        | ready | stat...
  ├───────────────────────────────────────────┼───────┼─────...
  | ydb://import/8?id=281474976788395&kind=s3 | true  | SUCC...
  ├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴...
  | Items:
  ...
  ```

{% include [import-operation-result-json-intro.md](import-operation-result-json-intro.md) %}

  ```json
  {"id":"ydb://import/8?id=281474976788395&kind=s3","ready":true, ... }
  ```

### Статус загрузки {#status}

{% include [import-operation-status-intro.md](import-operation-status-intro.md) %}

```bash
{{ ydb-cli }} -p quickstart operation get "ydb://import/8?id=281474976788395&kind=s3"
```

{% include [import-operation-status-after-get.md](import-operation-status-after-get.md) %}

### Завершение операции загрузки {#forget}

{% include [import-operation-forget-intro.md](import-operation-forget-intro.md) %}

```bash
{{ ydb-cli }} -p quickstart operation forget "ydb://import/8?id=281474976788395&kind=s3"
```

### Список операций загрузки {#list}

Для получения списка операций загрузки воспользуйтесь командой `operation list import/s3`:

```bash
{{ ydb-cli }} -p quickstart operation list import/s3
```

{% include [import-operation-list-tail.md](import-operation-list-tail.md) %}

## Примеры {#examples}

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

### Загрузка в корень базы данных {#example-full-db}

Загрузка в корень базы данных содержимого директории `export1` в бакете `mybucket` с использованием параметров аутентификации S3 из переменных окружения или файла `~/.aws/credentials`:

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --source-prefix export1
```

### Загрузка нескольких директорий {#example-specific-dirs}

Загрузка объектов из директорий `dir1` и `dir2` выгрузки, которая находится в директории `export1` в бакете `mybucket`, в одноименные директории базы данных с использованием явно заданных параметров аутентификации в S3:

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1
  --include dir1 --include dir2
```

### Перечисление объектов в существующей зашифрованной выгрузке {#example-list}

Перечисление путей всех объектов в существующей зашифрованной выгрузке, которая находится в директории `export1` в бакете `mybucket`, с использованием секретного ключа из файла `~/my_secret_key`.

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1
  --encryption-key-file ~/my_secret_key
  --list
```

### Загрузка зашифрованной выгрузки {#example-encryption}

Загрузка одной таблицы, которая была выгружена по пути `dir/my_table`, в путь `dir1/dir/my_table` из зашифрованной выгрузки, расположенной по префиксу `export1` в бакете `mybucket`, с использованием секретного ключа из файла `~/my_secret_key`.

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1 --destination-path dir1 \
  --include dir/my_table \
  --encryption-key-file ~/my_secret_key
```

### Получение идентификаторов операций {#example-list-oneline}

Для получения перечня идентификаторов операций загрузки в удобном для обработки в скриптах bash формате вы можете применить утилиту [jq](https://stedolan.github.io/jq/download/):

```bash
{{ ydb-cli }} -p quickstart operation list import/s3 --format proto-json-base64 | jq -r ".operations[].id"
```

Вы получите вывод, где в каждой новой строке находится идентификатор операции, например:

```text
ydb://import/8?id=281474976789577&kind=s3
ydb://import/8?id=281474976789526&kind=s3
ydb://import/8?id=281474976788779&kind=s3
```

По этим идентификаторам может быть, например, запущен цикл для завершения всех текущих операций:

```bash
{{ ydb-cli }} -p quickstart operation list import/s3 --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```
