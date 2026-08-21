# Импорт CSV в {{ ydb-short-name }} с помощью YDB CLI

Пошаговый рецепт — загрузка данных из **CSV-файла** в {{ ydb-short-name }} с помощью [YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

## Подготовка {#prerequisites}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

CSV-файл должен быть доступен с машины, где запускается CLI (локальный путь или смонтированное хранилище). Кодировка — UTF-8.

## Пошаговая инструкция {#steps}

Подробнее о команде: [import file в YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

Создайте таблицу вручную или сгенерируйте черновик DDL по файлу:

```bash
ydb -e grpc://localhost:2136 -d /local tools infer csv --header /path/to/data.csv
```

Пример DDL:

```yql
CREATE TABLE `mydb/users` (
    `id` Int64,
    `name` Text,
    PRIMARY KEY (`id`)
);
```

### Шаг 2. Импорт CSV

Если первая строка файла — заголовок с именами колонок:

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/users --header /path/to/data.csv
```

Без строки заголовка укажите имена колонок явно:

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/users --columns id,name /path/to/data.csv
```

Несколько файлов можно передать одной командой — см. [справочник import file](../../../../reference/ydb-cli/export-import/import-file.md).

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/users"
```
