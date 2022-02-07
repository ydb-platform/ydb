# Вторичные индексы

В разделе описано, как использовать [вторичные индексы](../../concepts/secondary_indexes.md) для выборки данных.

В общем случае транзакции с использованием глобального индекса являются [распределенными](../../concepts/transactions.md#distributed-tx). Запрос может быть выполнен как одношардовая транзакция в следующих случаях:

* точечное чтение по первичному ключу;
* точечное чтение по индексной колонке, если запрашиваемые данные представляют собой первичный ключ, его часть или имеется копия данных в индексе ([covering index](../../concepts/secondary_indexes.md#covering));
* точечная слепая запись в таблицу с [асинхронным индексом](../../concepts/secondary_indexes.md#async).

{% note warning %}

Размер ответа клиенту не может превышать 50 МБ. Размер данных, извлекаемых из одного шарда таблицы в одном YQL-запросе, не может превышать 5 ГБ. Для больших таблиц и запросов эти ограничения могут сделать невозможным полный просмотр всех строк таблицы.

{% endnote %}

## Создание таблицы {#create}

Чтобы создать таблицу `series`, выполните запрос:

```sql
CREATE TABLE series
(
    series_id Uint64,
    title Utf8,
    series_info Utf8,
    release_date Uint64,
    views Uint64,
    PRIMARY KEY (series_id),
    INDEX views_index GLOBAL ON (views)
);
```

Таблица `series` содержит ключевую колонку `series_id`. Индекс по первичному ключу в {{ ydb-short-name }} создается автоматически. Данные хранятся отсортированными по возрастанию значений первичного ключа, поэтому выборки по такому ключу будут эффективны. Пример такой выборки — поиск всех выпусков сериала по его `series_id` из таблицы `series`. Также создан индекс с именем `views_index` к колонке `views`, который позволит эффективно выполнять запросы используя ее в предикате.

Для более сложной выборки можно создать несколько вторичных индексов. Например, чтобы создать таблицу `series` с двумя индексами, выполните запрос:

```sql
CREATE TABLE series
(
    series_id Uint64,
    title Utf8,
    series_info Utf8,
    release_date Uint64,
    views Uint64,
    PRIMARY KEY (series_id),
    INDEX views_index GLOBAL ON (views) COVER (release_date),
    INDEX date_index GLOBAL ON (release_date)
);
```

Здесь созданы два вторичных индекса: `views_index` к колонке `views` и `date_index` к колонке `release_date`. Причем индекс `views_index` содержит копию данных из колонки `release_date`.

{% note info %}

Возможно добавление вторичного индекса для существующей таблицы без остановки обслуживания. Подробнее об онлайн-создании индекса читайте в [инструкции](../../concepts/secondary_indexes.md#index-add).

{% endnote %}

## Вставка данных {#insert}

В примерах YQL-запросов используются [prepared queries](https://en.wikipedia.org/wiki/Prepared_statement). Чтобы выполнить их в YQL-редакторе, задайте значения параметров, которые объявлены с помощью инструкции `DECLARE`.

Чтобы добавить данные в таблицу `series`, выполните запрос:

```sql
DECLARE $seriesId AS Uint64;
DECLARE $title AS Utf8;
DECLARE $seriesInfo AS Utf8;
DECLARE $releaseDate AS Uint32;
DECLARE $views AS Uint64;

INSERT INTO series (series_id, title, series_info, release_date, views)
VALUES ($seriesId, $title, $seriesInfo, $releaseDate, $views);
```

## Изменение данных {#upsert}

Сохранить в БД новое значение количества просмотров для определенного сериала можно с помощью операции `UPSERT`.

Чтобы изменить данные в таблице `series`, выполните запрос:

```sql
DECLARE $seriesId AS Uint64;
DECLARE $newViews AS Uint64;

UPSERT INTO series (series_id, views)
VALUES ($seriesId, $newViews);
```

## Выборка данных {#select}

Без использования вторичных индексов запрос на выборку записей, которые удовлетворяют заданному предикату по количеству просмотров, будет работать неэффективно, так как для его выполнения {{ ydb-short-name }} просканирует все строки таблицы `series`. В запросе необходимо явно указать, какой индекс использовать.

Чтобы выбрать строки из таблицы `series`, удовлетворяющие предикату по количеству просмотров, выполните запрос:

```sql
SELECT series_id, title, series_info, release_date, views
FROM series view views_index
WHERE views >= someValue
```

## Обновление данных с использованием вторичного индекса {#update}

Инструкция `UPDATE` не позволяет указать на использование вторичного индекса для поиска данных, поэтому попытка выполнить `UPDATE ... WHERE indexed_field = $value` приведет к полному сканированию таблицы. Чтобы избежать этого, можно предварительно выполнить `SELECT` по индексу с получением значения первичного ключа, а затем выполнить `UPDATE` по первичному ключу. Также можно воспользоваться инструкцией `UPDATE ON`.

Чтобы обновить данные в таблице `series`, выполните запрос:

```sql
$to_update = (
    SELECT pk_field, field1 = $f1, field2 = $f2, ...
    FROM   table1 view idx_field3
    WHERE  field3 = $f3)

UPDATE table1 ON SELECT * FROM $to_update
```

## Удаление данных с использованием вторичного индекса {#delete}

Для удаления данных по вторичному индексу используется `SELECT` c предикатом по вторичному индексу, а затем вызывается инструкция `DELETE ON`.

Чтобы удалить все данные о сериалах с нулевым количеством просмотров в таблице `series`, выполните запрос:

```sql
DELETE FROM series ON
SELECT series_id, 
FROM series view views_index
WHERE views == 0;
```
