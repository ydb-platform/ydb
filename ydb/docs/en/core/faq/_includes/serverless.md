# {{ ydb-short-name }} in Serverless mode

#### How do secondary indexes affect the request cost?

Operations with indexes are estimated according to the same rules as operations with tables. They are reflected in the request statistics and included in the total indicators that are used to calculate the cost in Request Units (RU). Read more in [pricing policy for the serverless {{ ydb-short-name }} API]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/ydb/pricing/request_units_yql){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/ydb/pricing/request_units_yql){% endif %}.

When reading data from a table using an index, the request statistics will show the number of rows read from the index and their volume.

When adding a new row to a table, a record is also added to each index that exists in this table, with the number of added records and the volume of written data shown in the statistics.

Whenever a table row update occurs, the statistics will reflect a deletion operation for the old record and an insert for the new one for all indexes that include the fields being updated.

When deleting a table row, the statistics will include the deletion of records from all indexes in this table.

