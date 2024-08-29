### Особенности поддержки типов с меткой временной зоны

Метка временной зоны у типов `TzDate`, `TzDatetime`, `TzTimestamp` это атрибут, который используется:

* При преобразовании ([CAST](../../syntax/expressions.md#cast), [DateTime::Parse](../../udf/list/datetime.md#parse), [DateTime::Format](../../udf/list/datetime.md#format)) в строку и из строки.
* В [DateTime::Split](../../udf/list/datetime.md#split) - появляется компонент таймзоны в `Resource<TM>`.

Само значение позиции во времени у этих типов хранится в UTC, и метка таймзоны никак не участвует в прочих расчётах. Например:
``` yql
select --эти выражения всегда true для любых таймзон: таймзона не влияет на точку во времени.
    AddTimezone(CurrentUtcDate(), "Europe/Moscow") ==
        AddTimezone(CurrentUtcDate(), "America/New_York"),
    AddTimezone(CurrentUtcDatetime(), "Europe/Moscow") ==
        AddTimezone(CurrentUtcDatetime(), "America/New_York");
```
Важно понимать, что при преобразованиях между `TzDate` и `TzDatetime` или `TzTimestamp` дате соответствует не полночь по локальному времени таймзоны, а полночь по UTC для даты в UTC.
