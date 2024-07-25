# Конвертация таблиц между строковой и столбцовой ориентацией

{{ ydb-short-name }} поддерживает два основных типа таблиц: [строчные](../../concepts/datamodel/table.md#row-oriented-tables) и [столбцовые](../../concepts/datamodel/table.md#column-oriented-tables). Выбранный тип таблицы определяет физическое представление данных на дисках, поэтому изменение типа на месте невозможно. Однако вы можете создать новую таблицу другого типа и скопировать в неё данные. Этот рецепт состоит из двух шагов:

1. [Подготовка новой таблицы](#prepare)
2. [Копирование данных](#copy)

В приведённом ниже тексте предполагается, что исходная таблица строчная и цель заключается в преобразовании её в столбцовую таблици, но роли могут быть и обратными.

{% include [ydb-cli-profile.md](../../_includes/ydb-cli-profile.md) %}

## Подготовка новой таблицы {#prepare}

Возьмите копию исходной команды `CREATE TABLE`, использованной для создания исходной таблицы. Измените следующее, чтобы создать файл с запросом `CREATE TABLE` для таблицы назначения:

1. Измените имя таблицы на неиспользуемое.
2. Измените значение параметра `STORE` на `COLUMN` (или добавьте его), чтобы сделать таблицу столбцовой.

Запустите этот запрос (предполагается, что он сохранён в файле с именем `create_column_oriented_table.yql`):

```bash
$ ydb -p quickstart yql -f create_column_oriented_table.yql
```

{% cut "Примеры схем тестовых таблиц и заполнения их данными" %}

Исходная строчная таблица:

```yql
CREATE TABLE `row_oriented_table` (
    id Int64 NOT NULL,
    metric_a Double,
    metric_b Double,
    metric_c Double,
    PRIMARY KEY (id)
)
```

Столбцовая таблица назначения:

```yql
CREATE TABLE `column_oriented_table` (
    id Int64 NOT NULL,
    metric_a Double,
    metric_b Double,
    metric_c Double,
    PRIMARY KEY (id)
)
PARTITION BY HASH(id)
WITH (STORE = COLUMN)
```

Заполнение исходной строчной таблицы случайными данными:

```yql
INSERT INTO `row_oriented_table` (id, metric_a, metric_b, metric_c)
SELECT
    id,
    Random(id + 1),
    Random(id + 2),
    Random(id + 3)
FROM (
    SELECT ListFromRange(1, 1000) AS id
) FLATTEN LIST BY id
```

{% endcut %}

## Копирование данных {#copy}

На данный момент рекомендуемый способ копирования данных между {{ ydb-short-name }} таблицами разных типов  — это экспорт и импорт:

1. Экспорт данных в локальную файловую систему:

```bash
$ ydb -p quickstart dump -p row_oriented_table -o tmp_backup/
```

2. Импорт этих данных в другую таблицу {{ ydb-short-name }}:

```bash
$ ydb -p quickstart import file csv -p column_oriented_table tmp_backup/row_oriented_table/*.csv
```

Если объём данных велик, рассмотрите возможность использования файловой системы, которая может с ним справиться.

## Смотрите также

* [{#T}](../../reference/ydb-cli/index.md)
* [{#T}](../../dev/index.md)