# Using the TRANSFER mechanism to implement SCD1 based on data from CDC sources in {{ ydb-full-name }}

This article describes the implementation of the Slowly Changing Dimensions Type 1 (SCD1) pattern in {{ ydb-short-name }}.

## Used tools

For delivering data to the SCD1 table, the following combination of functional capabilities of {{ ydb-short-name }} will be used in this article:

1. The source table will be [row-oriented](../../../concepts/datamodel/table.md#row-oriented-table) for operational transactional changes;
2. The target table will be [column-oriented](../../../concepts/datamodel/table.md#column-oriented-table) for efficient execution of analytical queries;
3. The subscription to changes in the source table will be implemented through the [Change Data Capture (CDC)](../../../concepts/cdc.md) mechanism;
4. The buffering of changes will be handled by an implicitly created CDC [topic](../../../concepts/datamodel/topic.md);
5. The automatic transfer of data from the CDC topic to the target table will be handled by [transfer](../../../concepts/transfer.md).

{% note warning %}

Transfer cannot delete rows from tables, it can only perform [`UPSERT`](../../../yql/reference/syntax/upsert_into.md) in the target table.
Therefore, using Transfer, only SCD1-soft can be implemented: deletion is marked with a flag and the moment of deletion is saved. If physical data cleaning is needed, it can be performed through an explicit command [`DELETE`](#howtodelete).

{% endnote %}

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

  ## Create a target data table in SCD1 format

  ```sql
  CREATE TABLE dimension_scd1 (
      id Utf8 NOT NULL,
      attribute1 Utf8,
      attribute2 Utf8,
      is_deleted Uint8,
      last_update_timestamp Timestamp,
      PRIMARY KEY (id)
  )
  PARTITION BY HASH(id)
  WITH (
    STORE=COLUMN
  )
  ```

  ## Create a transfer and lambda function to process CDC data

  Features of processing CDC data in Debezium format:

  * The message format contains the `payload.before` and `payload.after` fields with the state before and after the change.
  * The `payload.op` field indicates the type of operation: "c" (create), "u" (update), "d" (delete).
  * The `payload.ts_ms` field contains the event timestamp in milliseconds.
  * When deleting (`op = "d"`), the data is in the `before` field, and when creating/updating - in the `after` field. If deletion of a non-existent row is performed, a message with the operation type `op = "d"` will be formed, but with empty `before` and `after` fields.

  ```sql
  $transformation_lambda = ($msg) -> {
      $cdc_data = CAST($msg._data AS Json);

      -- Define the type of operation
      $operation = Json::ConvertToString($cdc_data.payload.op);
      $is_deleted = $operation == "d";

      -- Get data depending on the type of operation
      $data = IF($is_deleted, $cdc_data.payload.before, $cdc_data.payload.after);

      -- If the data has not come (the DELETE command has been executed on a non-existent key, then the record will be ignored)
      return IF($data IS NOT NULL,
          <|
              id: Unwrap(CAST(Json::ConvertToString($data.id) AS Utf8)),
              attribute1: CAST(Json::ConvertToString($data.attribute1) AS Utf8),
              attribute2: CAST(Json::ConvertToString($data.attribute2) AS Utf8),
              is_deleted: CAST($is_deleted AS Uint8),
              last_update_timestamp: DateTime::FromMilliseconds(Json::ConvertToUint64($cdc_data.payload.source.ts_ms))
        |>
      , NULL);
  };

  -- In this example, a high frequency of updates to the target table is configured.
  -- This is done solely for clarity. For production scenarios, larger values should be set
  CREATE TRANSFER dimension_scd1_cdc_transfer
    FROM `source_customers/updates` TO dimension_scd1 USING $transformation_lambda
    WITH (
      FLUSH_INTERVAL=Interval("PT1S")
    );
  ```

  {% note info %}

  The `id` column in the receiving table `dimension_scd1` is declared as `Utf8 NOT NULL`, while in Json CDC, data that cannot be converted to a string may be passed, i.e., the result of converting data from Json may be a `NULL` value. The [`Unwrap`](../../../yql/reference/builtins/basic.md#unwrap) function guarantees that after its execution, there cannot be a `NULL` value or a runtime error. This ensures that the result of executing the lambda function will either be a fully correct data structure, or a runtime error.
  
  {% endnote %}

  ## Demonstration of work

  To demonstrate the work with CDC data, data will be written to the source table in {{ ydb-short-name }}, which will generate CDC events:

  ### Inserting a new record

  ```sql
  INSERT INTO source_customers (id, attribute1, attribute2)
  VALUES ('CUSTOMER_1001', 'John Doe', 'New York');
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
        "step":1755868513260,
        "ts_ms":1755868513219,
        "snapshot":false
      },
      "after":{
        "attribute1":"John Doe",
        "id":"CUSTOMER_1001",
        "attribute2":"New York"
      }
    }
  }
  ```

  The content of the `dimension_scd1` table after executing the command above will be as follows:

  | id             | attribute1 | attribute2    | is\_deleted | last_update_timestamp       |
  | -------------- | ---------- | ------------- | ----------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | New York      | 0           | 2025-08-22T13:15:13.219000Z |

  ### Updating an existing record

  ```sql
  UPSERT INTO source_customers (id, attribute1, attribute2)
  VALUES ('CUSTOMER_1001', 'John Doe', 'Los Angeles');
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
          "step":1755868795050,
          "ts_ms":1755868795719,
          "snapshot":false
      },
      "after":{
        "attribute1":"John Doe",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles"
      },
      "before":{
        "attribute1":"John Doe",
        "id":"CUSTOMER_1001",
        "attribute2":"New York"
      }
    }
  }
  ```

  The content of the `dimension_scd1` table after executing the command above will be as follows:

  | id             | attribute1 | attribute2    | is\_deleted | last_update_timestamp       |
  | -------------- | ---------- | ------------- | ----------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | Los Angeles   | 0           | 2025-08-22T13:19:55.719000Z |


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
        "step":1755868931000,
        "ts_ms":1755868931109,
        "snapshot":false
      },
      "before":{
        "attribute1":"John Doe",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles"
      }
    }
  }
  ```

  The content of the `dimension_scd1` table after executing the command above will be as follows:

  | id             | attribute1 | attribute2    | is\_deleted | last_update_timestamp       |
  | -------------- | ---------- | ------------- | ----------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | Los Angeles   | 1           | 2025-08-22T13:22:11.109000Z |


## Example of deleting data from the target table {#howtodelete}

To delete data from the target table, you can use an explicit `DELETE` command:

```sql
DELETE from dimension_scd1 WHERE is_deleted != 0ut
```