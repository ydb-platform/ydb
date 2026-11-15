### Сигнатура

```yql
RemoveTimezone(TzDate)->Date
RemoveTimezone(TzDate?)->Date?
RemoveTimezone(TzDatetime)->Datetime
RemoveTimezone(TzDatetime?)->Datetime?
RemoveTimezone(TzTimestamp)->Timestamp
RemoveTimezone(TzTimestamp?)->Timestamp?
```

Аргументы:

1. Дата - тип `TzDate`/`TzDatetime`/`TzTimestamp`.

Тип результата - `Date`/`Datetime`/`Timestamp`, в зависимости от типа данных входа.

### Примеры
