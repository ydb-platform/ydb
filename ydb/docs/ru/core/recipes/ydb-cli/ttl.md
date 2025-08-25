# Настройка времени жизни строк (TTL) таблицы

В этом разделе приведены примеры настройки TTL строковых и колоночных таблиц при помощи {{ ydb-short-name }} CLI.

## Включение TTL для существующих строковых и колоночных таблиц {#enable-on-existent-table}

В приведенном ниже примере строки таблицы `mytable` будут удаляться спустя час после наступления времени, записанного в колонке `created_at`:

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> table ttl set --column created_at --expire-after 3600 mytable
```

Следующий пример демонстрирует использование колонки `modified_at` с числовым типом (`Uint32`) в качестве TTL-колонки. Значение колонки интерпретируется как секунды от Unix-эпохи:

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> table ttl set --column modified_at --expire-after 3600 --unit seconds mytable
```

## Включение вытеснения данных во внешнее S3-совместимое хранилище

{% include [OLTP_not_allow_note](../../_includes/not_allow_for_oltp_note.md) %}

Для включения вытеснения требуется объект [external data source](../../concepts/datamodel/external_data_source.md), описывающий подключение к внешнему хранилищу. Пример создания объекта можно найти в [рецептах YQL](../../yql/reference/recipes/ttl.md#enable-tiering-on-existing-tables).

Следующий пример демонстрирует включение вытеснения данных через вызов YQL-запроса из {{ ydb-short-name }} CLI. Строки таблицы `mytable` будут переноситься в бакет, описанный во внешнем источнике данных `/Root/s3_cold_data`, спустя час после наступления времени, записанного в колонке `created_at`, а спустя 24 часа будут удаляться.

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> sql -s '
    ALTER TABLE `mytable` SET (
        TTL =
            Interval("PT1H") TO EXTERNAL DATA SOURCE `/Root/s3_cold_data`,
            Interval("PT24H") DELETE
        ON modified_at AS SECONDS
    );
'
```

## Выключение TTL {#disable}

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> table ttl reset mytable
```

## Получение настроек TTL {#describe}

Текущие настройки TTL можно получить из описания таблицы:

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> scheme describe mytable
```

