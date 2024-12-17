## CurrentUtc... {#current-utc}

`CurrentUtcDate()`, `CurrentUtcDatetime()` и `CurrentUtcTimestamp()` - получение текущей даты и/или времени в UTC. Тип данных результата указан в конце названия функции.

### Сигнатуры

```yql
CurrentUtcDate(...)->Date
CurrentUtcDatetime(...)->Datetime
CurrentUtcTimestamp(...)->Timestamp
```

Аргументы опциональны и работают по тому же принципу, что и у [RANDOM](../../basic.md#random).

### Примеры

```yql
SELECT CurrentUtcDate();
```

```yql
SELECT CurrentUtcTimestamp(TableRow()) FROM my_table;
```
