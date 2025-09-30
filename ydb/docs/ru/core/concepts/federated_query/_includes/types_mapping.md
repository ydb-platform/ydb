|YQL тип|Экспорт arrow тип|Импорт arrow тип|Комментарий|
|----|----|----|---|
|`Bool`|`UINT8`|`BOOL`, `UINT8`||
|`Int8`|`INT8`|`INT8`||
|`Int16`|`INT16`|`INT16`||
|`Int32`|`INT32`|`INT32`||
|`Int64`|`INT64`|`INT64`||
|`Uint8`|`UINT8`|`UINT8`||
|`Uint16`|`UINT16`|`UINT16`||
|`Uint32`|`UINT32`|`UINT32`||
|`Uint64`|`UINT64`|`UINT64`||
|`Float`|`FLOAT`|`FLOAT`||
|`Double`|`DOUBLE`|`DOUBLE`||
|`Date`|`UINT16`|`UINT16`, `INT32`, `UINT32`, `INT64`, `UINT64`, `DATE`, `TIMESTAMP (s, ms, us)`|количество дней, прошедших с начала [Unix-эпохи](https://ru.wikipedia.org/wiki/Unix-время)|
|`Date32`|`DATE (s)`|`DATE (s)`||
|`Datetime`|`UINT32`|`UINT16`, `INT32`, `UINT32`, `INT64`, `UINT64`, `DATE`, `TIMESTAMP (s, ms, us)`|количество секунд, прошедших с начала [Unix-эпохи](https://ru.wikipedia.org/wiki/Unix-время)|
|`Datetime64`|`TIMESTAMP (us)`|`TIMESTAMP (s, ms, us)`||
|`Timestamp`|`TIMESTAMP (us)`|`TIMESTAMP (s, ms, us)`||
|`Timestamp64`|`TIMESTAMP (us)`|`TIMESTAMP (s, ms, us)`||
|`Decimal(n, m)`|`DECIMAL128 (n, m)`|`DECIMAL128 (n, m)`||
|`String`|`BINARY`|`BINARY`||
|`Utf8`|`BINARY`|`BINARY`||
|`Json`|`BINARY`|`BINARY`||
