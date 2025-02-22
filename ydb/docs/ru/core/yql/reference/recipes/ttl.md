# Настройка времени жизни строк (TTL) таблицы

В этом разделе приведены примеры настройки TTL строковых и колоночных таблиц при помощи YQL.

## Включение TTL для существующих строковых и колоночных таблиц {#enable-on-existent-table}

В приведенном ниже примере строки таблицы `mytable` будут удаляться спустя час после наступления времени, записанного в колонке `created_at`:

```yql
ALTER TABLE `mytable` SET (TTL = Interval("PT1H") ON created_at);
```

{% note tip %}

`Interval` создается из строкового литерала в формате [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) с [некоторыми ограничениями](../builtins/basic#data-type-literals).

{% endnote %}

Следующий пример демонстрирует использование колонки `modified_at` с числовым типом (`Uint32`) в качестве TTL-колонки. Значение колонки интерпретируется как секунды от Unix-эпохи:

```yql
ALTER TABLE `mytable` SET (TTL = Interval("PT1H") ON modified_at AS SECONDS);
```

## Включение вытеснения данных во внешнее S3-совместимое хранилище {#enable-tiering-on-existing-tables}

{% include [OLTP_not_allow_note](../../../_includes/not_allow_for_oltp_note.md) %}

Для включения вытеснения требуется объект [external data source](../../../concepts/datamodel/external_data_source.md), описывающий подключение к внешнему хранилищу.
В приведённом ниже примере создаётся external data source `/Root/s3_cold_data`: он описывает подключение к бакету `test_cold_data`, расположенному в Yandex Object Storage, с авторизацией через статический ключ доступа, данные которого хранятся в секретах `access_key` и `secret_key`.

```yql
CREATE OBJECT access_key (TYPE SECRET) WITH (value="...");
CREATE OBJECT secret_key (TYPE SECRET) WITH (value="...");

CREATE EXTERNAL DATA SOURCE `/Root/s3_cold_data` WITH (
    SOURCE_TYPE="ObjectStorage",
    AUTH_METHOD="AWS",
    LOCATION="http://storage.yandexcloud.net/test_cold_data",
    AWS_ACCESS_KEY_ID_SECRET_NAME="access_key",
    AWS_SECRET_ACCESS_KEY_SECRET_NAME="secret_key",
    AWS_REGION="ru-central1"
)
```

Используя объект external data source, можно включить вытеснение данных следующим образом.

В следующем примере строки таблицы `mytable` будут переноситься в бакет, описанный во внешнем источнике данных `/Root/s3_cold_data`, спустя час после наступления времени, записанного в колонке `created_at`, а спустя 24 часа будут удаляться:

```yql
ALTER TABLE `mytable` SET (
    TTL =
        Interval("PT1H") TO EXTERNAL DATA SOURCE `/Root/s3_cold_data`,
        Interval("PT24H") DELETE
    ON modified_at AS SECONDS
);
```

В следующем примере строки таблицы `mytable` будут переноситься в бакет `/Root/s3_cold` через час и в бакет `/Root/s3_frozen` через 30 дней после наступления времени, записанного в колонке `created_at`:

```yql
ALTER TABLE `mytable` SET (
    TTL =
        Interval("PT1H") TO EXTERNAL DATA SOURCE `/Root/s3_cold`,
        Interval("PT30D") TO EXTERNAL DATA SOURCE `/Root/s3_frozen`
    ON modified_at AS SECONDS
);
```

## Включение TTL для вновь создаваемой таблицы {#enable-for-new-table}

Для вновь создаваемой таблицы можно передать настройки TTL вместе с ее описанием:

```yql
CREATE TABLE `mytable` (
  id Uint64,
  expire_at Timestamp,
  PRIMARY KEY (id)
) WITH (
  TTL = Interval("PT0S") ON expire_at
);
```

## Выключение TTL {#disable}

```yql
ALTER TABLE `mytable` RESET (TTL);
```

