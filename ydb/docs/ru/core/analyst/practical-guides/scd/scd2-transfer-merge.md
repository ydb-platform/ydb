# Реализация SCD2 (append-only) с использованием трансфера и операций слияния

В этой статье описывается реализация паттерна Slowly Changing Dimensions Type 2 (SCD2) в YDB.

## Используемые инструменты

Для поставки данных в SCD2 таблицу в данной статье будет использоваться следующая комбинация из доступной в {{ ydb-short-name }} функциональности:

1. Таблица-источник будет [строковой](../../../concepts/datamodel/table.md#row-oriented-table) для оперативных транзакционных изменений;
2. Таблица-приёмник будет [колоночной](../../../concepts/datamodel/table.md#column-oriented-table) для эффективного выполнения аналитических запросов;
3. Подписка на изменения в таблице-источники будет осуществляться через механизм [Change Data Capture (CDC)](../../../concepts/cdc.md);
4. За буферизацию изменений будет отвечать неявно создаваемый под CDC [топик](../../../concepts/datamodel/topic.md);
5. За автоматическое перекладывание данных из CDC-топика в таблицу-приёмник будет отвечать [трансфер](../../../concepts/transfer.md).

## Создайте таблицу-источник данных, которая будет генерировать CDC-события

  ```sql
  CREATE TABLE source_customers (
      id Utf8 NOT NULL,
      attribute1 Utf8,
      attribute2 Utf8,
      PRIMARY KEY (id)
  );

  ALTER TABLE `source_customers` ADD CHANGEFEED `updates` WITH (
    FORMAT = 'DEBEZIUM_JSON',
    MODE = 'NEW_AND_OLD_IMAGES'
  );
  ```

## Создайте таблицу для приема изменений

  ```sql
    CREATE TABLE dimension_scd_changes (
        id Utf8 NOT NULL,
        attribute1 Utf8,
        attribute2 Utf8,
        change_time Timestamp NOT NULL,
        operation Utf8,
        row_operation_at Timestamp ,
        PRIMARY KEY (change_time, id)
    )
    PARTITION BY HASH(change_time, id)
    WITH (
      STORE=COLUMN
    )
  ```

## Создайте таблицу для хранения данных

  ```sql
    CREATE TABLE dimension_scd2_final (
        id Utf8 NOT NULL,
        attribute1 Utf8,
        attribute2 Utf8,
        valid_from Timestamp NOT NULL,
        valid_to Timestamp,
        is_current Timestamp ,
        is_deleted Uint8,
        PRIMARY KEY (valid_from, id)
    )
    PARTITION BY HASH(valid_from, id)
    WITH (
      STORE=COLUMN
    )
  ```

## Создайте трансфер для переноса данных

  ```sql
  $transformation_lambda = ($msg) -> {
    $cdc_data = CAST($msg._data AS Json);

    -- Определяем тип операции
    $operation = Json::ConvertToString($cdc_data.payload.op);
    $is_deleted = $operation == "d";

    $operation_type = CASE $operation
                            WHEN 'd' THEN "DELETE"u
                            WHEN 'u' THEN "UPDATE"u
                            WHEN 'c' THEN "CREATE"u
                            ELSE NULL
                        END;

    -- Получаем данные в зависимости от типа операции
    $data = IF($is_deleted, $cdc_data.payload.before, $cdc_data.payload.after);

    return IF($data is not NULL, <|
            id: Unwrap(CAST(Json::ConvertToString($data.id) AS Utf8)),
            attribute1: CAST(Json::ConvertToString($data.attribute1) AS Utf8),
            attribute2: CAST(Json::ConvertToString($data.attribute2) AS Utf8),
            change_time : Unwrap(cast(datetime::MakeDatetime(datetime::ParseIso8601(Json::ConvertToString($data.change_time))) AS Timestamp)),
            operation: Unwrap($operation_type)
        |>, NULL);
  };

  CREATE TRANSFER dimension_scd2_cdc_changes
    FROM `source_customers/updates` TO dimension_scd_changes USING $transformation_lambda
    WITH (
      FLUSH_INTERVAL=Interval("PT1S")
    )
    ;

  ```

## Создайте периодический запрос для подмерживания изменений из таблицы `dimension_scd_changes` в основную таблицу хранения данных в формате SCD2
