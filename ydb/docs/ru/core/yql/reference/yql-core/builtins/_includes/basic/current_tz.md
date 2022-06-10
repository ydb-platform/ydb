## CurrentTz... {#current-tz}

`CurrentTzDate()`, `CurrentTzDatetime()` и `CurrentTzTimestamp()` - получение текущей даты и/или времени в указанной в первом аргументе [IANA временной зоне](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones). Тип данных результата указан в конце названия функции.

**Сигнатуры**
```
CurrentTzDate(String, ...)->TzDate
CurrentTzDatetime(String, ...)->TzDatetime
CurrentTzTimestamp(String, ...)->TzTimestamp
```

Последующие аргументы опциональны и работают по тому же принципу, что и у [RANDOM](#random).

**Примеры**
``` yql
SELECT CurrentTzDate("Europe/Moscow");
```
``` yql
SELECT CurrentTzTimestamp("Europe/Moscow", TableRow()) FROM my_table;
```

## AddTimezone

Добавление информации о временной зоне к дате/времени, заданных в UTC. При выводе в результате `SELECT` или после `CAST` в `String` будут применены правила временной зоны по вычислению смещения времени.

**Сигнатура**
```
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

**Примеры**
``` yql
SELECT AddTimezone(Datetime("2018-02-01T12:00:00Z"), "Europe/Moscow");
```

## RemoveTimezone

Удаление информации о временной зоне и перевод в дату/время, заданные в UTC.

**Сигнатура**
```
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

**Примеры**
``` yql
SELECT RemoveTimezone(TzDatetime("2018-02-01T12:00:00,Europe/Moscow"));
```
