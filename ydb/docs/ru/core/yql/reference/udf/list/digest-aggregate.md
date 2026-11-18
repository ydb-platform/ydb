# DigestAggregate

`DigestAggregate` вычисляет контрольную сумму строк, выбранных запросом. Используйте его, чтобы отфильтровать строки, выбрать столбцы или получить отдельную контрольную сумму для каждой группы. Для проверки всей таблицы используйте [`CHECKSUM TABLE`](../../syntax/checksum-table.md).

## Подготовьте запрос {#factory}

Вставьте код вспомогательной функции ниже, затем SQL-запрос из одного из [примеров](#examples). Выполните их вместе. Функция `$DigestFactory` задаёт способ вычисления контрольной суммы для [`AGGREGATE_BY`](../../builtins/aggregation.md#aggregate-by).

{% cut "Определение функции: скопируйте этот блок без изменений" %}

```yql
PRAGMA Warning("disable", "4510");

$DigestFactory = ($algorithm) -> {
    $add = ($state, $row) -> {
        $bytes = YQL::IfType(
            $row, TypeOf(DigestAggregate::Canonicalize($row)),
            ($value) -> (StablePickle($value)),
            ($value) -> (StablePickle(DigestAggregate::Canonicalize($value)))
        );
        RETURN CASE $algorithm
            WHEN "CityHash64" THEN DigestAggregate::AggregateAddValue($state, Digest::CityHash($bytes))
            WHEN "CityHash128" THEN DigestAggregate::AggregateAddValue($state, Digest::CityHash128($bytes))
            ELSE DigestAggregate::AggregateAddValue($state, Digest::Sha256($bytes))
        END;
    };

    RETURN AggregationFactory(
        "UDAF",
        ($row, $_parent) -> {
            RETURN $add(DigestAggregate::AggregateCreate($algorithm), $row);
        },
        ($state, $row, $_parent) -> { RETURN $add($state, $row); },
        DigestAggregate::AggregateMerge,
        DigestAggregate::AggregateFinalize,
        DigestAggregate::AggregateSerialize,
        DigestAggregate::AggregateDeserialize,
        DigestAggregate::AggregateFinalize(DigestAggregate::AggregateCreate($algorithm))
    );
};
```

{% endcut %}

## Примеры {#examples}

В примерах используется таблица `my_table` со следующими данными. Оба столбца объявлены как `Uint64 NOT NULL`: они хранят неотрицательные целые числа и не допускают `NULL`. Столбец `customer_id` содержит идентификатор клиента.

| customer_id | value |
| --- | --- |
| 10 | 1 |
| 20 | 2 |
| 20 | 3 |

### Контрольная сумма для одного клиента

Вычислим контрольную сумму столбца `value` для клиента `20`. Условие [`WHERE`](../../syntax/select/where.md) выбирает строки, а [`AsStruct`](../../builtins/basic.md#as-container) — столбец для вычисления контрольной суммы:

```yql
SELECT
    AGGREGATE_BY(
        AsStruct(value AS value),
        $DigestFactory("CityHash64")
    ) AS digest
FROM my_table
WHERE customer_id = 20ul;
```

Запрос возвращает одну строку. Значение `digest` показано в формате JSON:

| digest |
| --- |
| `{"algorithm": "CityHash64", "count": 2, "hash": "8B93FE91E401CF0E", "version": 1}` |

Контрольная сумма вычислена для значений `2` и `3`, поэтому `count` равен `2`.

### Контрольная сумма для каждого клиента

Чтобы вычислить отдельную контрольную сумму столбца `value` для каждого клиента, используйте [`GROUP BY`](../../syntax/select/group-by.md). [`ORDER BY`](../../syntax/select/order_by.md) сортирует результат по идентификатору клиента:

```yql
SELECT
    customer_id,
    AGGREGATE_BY(
        AsStruct(value AS value),
        $DigestFactory("CityHash64")
    ) AS digest
FROM my_table
GROUP BY customer_id
ORDER BY customer_id;
```

Запрос возвращает по одной строке для каждого клиента. Значения `digest` показаны в формате JSON:

| customer_id | digest |
| --- | --- |
| 10 | `{"algorithm": "CityHash64", "count": 1, "hash": "F453CDDF6A61C6A2", "version": 1}` |
| 20 | `{"algorithm": "CityHash64", "count": 2, "hash": "8B93FE91E401CF0E", "version": 1}` |

У клиента `10` одно значение (`1`), а у клиента `20` — два (`2` и `3`). В расчёт каждой контрольной суммы входит только `value`, а `customer_id` определяет группу.

## Формат результата

Каждое значение `digest` содержит название алгоритма (`algorithm`), количество строк (`count`), контрольную сумму (`hash`) и версию формата (`version`). Эти поля описаны в [справке по `CHECKSUM TABLE`](../../syntax/checksum-table.md#result).

Контрольная сумма записана в шестнадцатеричном формате с заглавными буквами, как в результате [`String::HexEncode`](string.md): 16 символов для `CityHash64`, 32 для `CityHash128` или 64 для `Sha256`.

Если фильтру в первом примере не соответствует ни одна строка, запрос всё равно вернёт одну строку: `count` будет равен `0`, а контрольная сумма будет состоять только из символов `0`.

Порядок строк не влияет на контрольную сумму.
