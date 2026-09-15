# Using the TRANSFER mechanism to implement SCD2 based on data from CDC sources in {{ ydb-full-name }}

This article describes the implementation of the Slowly Changing Dimensions Type 2 (SCD2) pattern in {{ ydb-short-name }}.

## Used tools

For delivering data to the SCD2 table, the following combination of functional capabilities of {{ ydb-short-name }} will be used in this article:

1. The source table will be [row-oriented](../../../concepts/datamodel/table.md#row-oriented-table) for operational transactional changes;
2. The target table will be [column-oriented](../../../concepts/datamodel/table.md#column-oriented-table) for efficient execution of analytical queries;
3. The subscription to changes in the source table will be implemented through the [Change Data Capture (CDC)](../../../concepts/cdc.md) mechanism;
4. The buffering of changes will be handled by an implicitly created CDC [topic](../../../concepts/datamodel/topic.md);
5. The automatic transfer of data from the CDC topic to the target table will be handled by [transfer](../../../concepts/transfer.md).

## Create a source data table that will generate CDC events

```sql
CREATE TABLE source_customers (
    id Utf8 NOT NULL,
    attribute1 Utf8,
    attribute2 Utf8,
    change_time Timestamp,
    PRIMARY KEY (id)
);

ALTER TABLE `source_customers` ADD CHANGEFEED `updates` WITH (
  FORMAT = 'DEBEZIUM_JSON',
  MODE = 'NEW_AND_OLD_IMAGES'
);
```

## Create a target data table in SCD2 format

  ```sql
    CREATE TABLE dimension_scd2 (
      id Utf8 NOT NULL,
      attribute1 Utf8,
      attribute2 Utf8,
      valid_from Timestamp NOT NULL,
      deleted_at Timestamp ,
      PRIMARY KEY (valid_from, id)
    )
    PARTITION BY HASH(valid_from, id)
    WITH (
      STORE=COLUMN
    )
  ```

## Create a transfer and lambda function to process CDC data

  Features of processing CDC data in Debezium format:

  * When creating a record (`op = "c"`), data is taken from the `after` field, and the value of the moment of change of the record from the `changed_at` field is saved in the `valid_from` field.
  * When updating a record (`op = "u"`), data is also taken from the `after` field, and the value of the moment of change of the record from the `changed_at` field is saved in the `valid_from` field.
  * When deleting a record (`op = "d"`), data is taken from the `before` field, and the system moment of time of receiving information about the deletion of the record is set in the `deleted_at` field.

  ```sql
  $transformation_lambda = ($msg) -> {
  $cdc_data = CAST($msg._data AS Json);

  -- Define the type of operation
  $operation = Json::ConvertToString($cdc_data.payload.op);
  $is_deleted = $operation == "d";

  -- Get data depending on the type of operation
  $data = IF($is_deleted, $cdc_data.payload.before, $cdc_data.payload.after);

  $result =  IF($data is not NULL, <|
          id: Unwrap(CAST(Json::ConvertToString($data.id) AS Utf8)),
          attribute1: CAST(Json::ConvertToString($data.attribute1) AS Utf8),
          attribute2: CAST(Json::ConvertToString($data.attribute2) AS Utf8),
          deleted_at: IF($is_deleted, DateTime::FromMilliseconds(Json::ConvertToUint64($cdc_data.payload.source.ts_ms)), NULL),
          valid_from : Unwrap(cast(datetime::MakeDatetime(datetime::ParseIso8601(Json::ConvertToString($data.change_time))) as timestamp)),
      |>, NULL);

  return $result;
  };

  -- In this example, a high frequency of updates to the target table is configured.
  -- This is done solely for clarity. For production scenarios, larger values should be set
    CREATE TRANSFER dimension_scd2_cdc_transfer
      FROM `source_customers/updates` TO dimension_scd2 USING $transformation_lambda
      WITH (
        FLUSH_INTERVAL=Interval("PT1S")
      );
  ```

  {% note info %}

  The `id` column in the receiving table `dimension_scd2` is declared as `Utf8 NOT NULL`, while in Json CDC, data that cannot be converted to a string may be passed, i.e., the result of converting data from Json may be a `NULL` value. The [`Unwrap`](../../../yql/reference/builtins/basic.md#unwrap) function guarantees that after its execution, there cannot be a `NULL` value or a runtime error. This ensures that the result of executing the lambda function will either be a fully correct data structure, or a runtime error.

  {% endnote %}

  ## Demonstration of work

  To demonstrate the work with CDC data, data will be written to the source table in {{ ydb-short-name }}, which will generate CDC events:

  ### Inserting a new record

  ```sql
  INSERT INTO source_customers (id, attribute1, attribute2, change_time)
  VALUES ('CUSTOMER_1001', 'John Doe', 'Los Angeles', CurrentUtcTimestamp());
  ```

  This action will create a CDC event with the operation type `op = "c"` approximately of the following form:

  ```json
  {
    "payload":{
      "op":"c",
      "source":{
        "txId":18446744073709551615,
        "connector":"ydb",
        "version":"1.0.0",
        "step":1755883683690,
        "ts_ms":1755883683652,
        "snapshot":false
      },
      "after":{
        "attribute1":"John Doe",
        "change_time":"2025-08-22T17:28:03.648313Z",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles"
      }
    }
  }
  ```

  The content of the `dimension_scd2` table after executing the command above will be as follows:

  | id             | attribute1 | attribute2    | deleted\_at                 | valid_from                  |
  | -------------- | ---------- | ------------- | --------------------------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | New York      | NULL                        | 2025-08-22T17:28:03.648313Z |

  ### Updating an existing record

  ```sql
  UPSERT INTO source_customers (id, attribute1, attribute2, change_time)
  VALUES ('CUSTOMER_1001', 'John Doe 2', 'Los Angeles 2', CurrentUtcTimestamp());
  ```

  This action will create a CDC event with the operation type `op = "u"` approximately of the following form:

  ```json
  {
    "payload":{
      "op":"u",
      "source":{
        "txId":18446744073709551615,
        "connector":"ydb",
        "version":"1.0.0",
        "step":1755883278000,
        "ts_ms":1755883278357,
        "snapshot":false
      },
      "after":{
        "attribute1":"John Doe 2",
        "change_time":"2025-08-22T17:31:18.357503Z",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles 2"
      },
      "before":{
        "attribute1":"John Doe 2",
        "change_time":"2025-08-22T17:28:03.648313Z",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles 2"
      }
    }
  }
  ```

  The content of the `dimension_scd2` table after executing the command above will be as follows:

  | id             | attribute1 | attribute2    | deleted\_at                 | valid_from                  |
  | -------------- | ---------- | ------------- | --------------------------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | New York      | NULL                        | 2025-08-22T17:28:03.648313Z |
  | CUSTOMER\_1001 | John Doe 2 | New York 2    | NULL                        | 2025-08-22T17:31:18.357503Z |

  ### Deleting a record

  ```sql
  DELETE FROM source_customers WHERE id = 'CUSTOMER_1001';
  ```

  This action will create a CDC event with the operation type `op = "d"` approximately of the following form:

  ```json
  {
    "payload":{
      "op":"d",
      "source":{
        "txId":18446744073709551615,
        "connector":"ydb",
        "version":"1.0.0",
        "step":1755883827000,
        "ts_ms":1755883827948,
        "snapshot":false
      },
      "before":{
        "attribute1":"John Doe",
        "change_time":"2025-08-22T17:38:03.648313Z",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles"
      }
    }
  }
  ```

  The content of the `dimension_scd2` table after executing the command above will be as follows:

  | id             | attribute1 | attribute2    | deleted\_at                 | valid_from                  |
  | -------------- | ---------- | ------------- | --------------------------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | New York      | NULL                        | 2025-08-22T17:28:03.648313Z |
  | CUSTOMER\_1001 | John Doe 2 | New York 2    | NULL                        | 2025-08-22T17:31:18.357503Z |
  | CUSTOMER\_1001 | John Doe 2 | New York 2    | 2025-08-22T17:38:03.648313Z | 2025-08-22T17:38:03.648313Z |

  ### Example query to retrieve current data

  To retrieve data from SCD2 append‑only tables, you can use the following query, which fetches data as of `2025-08-22 19:11:30`:

  ```sql
  DECLARE $as_of AS Timestamp;
  $as_of = Timestamp("2025-08-22T19:11:30.000000Z");

  SELECT
    id,
    data.attribute1 AS attribute1,
    data.attribute2 AS attribute2,
    data.valid_from AS valid_from,
    data.deleted_at AS deleted_at
  FROM (
    SELECT
      id,
      MAX_BY(TableRow(), valid_from) AS data
    FROM dimension_scd2
    WHERE
        valid_from <= $as_of
        AND (deleted_at IS NULL OR deleted_at > $as_of)  -- The record has not yet been deleted as of time X
    GROUP BY id
  )
  ```
