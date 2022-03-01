# {{ ydb-short-name }} in Serverless mode

#### How do secondary indexes affect the request cost?

Operations with indexes are estimated according to the same rules as operations with tables. They are reflected in the request statistics and included in the total indicators that are used to calculate the cost in Request Units (RU). For more information, see the [pricing policy for the serverless {{ ydb-short-name }} API](https://cloud.yandex.com/en/docs/ydb/pricing/request_units_yql).

When reading data from a table using an index, the request statistics will show the number of rows read from the index and their volume.

When adding a new row to a table, a record is also added to each index that exists in this table, with the number of added records and the volume of written data shown in the statistics.

When changing a table row, the operation of deleting the old record from the index and adding a new one will be reflected in the statistics for all indexes that include updated fields.

When deleting a table row, the statistics will include the deletion of records from all indexes in this table.

