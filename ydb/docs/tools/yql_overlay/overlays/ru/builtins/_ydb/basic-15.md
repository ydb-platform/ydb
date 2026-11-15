### Сигнатура

```yql
AddTimezone(Date, String)->TzDate
AddTimezone(Date?, String)->TzDate?
AddTimezone(Datetime, String)->TzDatetime
AddTimezone(Datetime?, String)->TzDatetime?
AddTimezone(Timestamp, String)->TzTimestamp
AddTimezone(Timestamp?, String)->TzTimestamp?
```

Аргументы:

1. Дата - тип `Date`/`Datetime`/`Timestamp`;
2. [IANA имя временной зоны](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).

Тип результата - `TzDate`/`TzDatetime`/`TzTimestamp`, в зависимости от типа данных входа.

### Примеры
