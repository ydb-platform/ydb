# Постраничный вывод

В разделе приведены рекомендации по организации постраничного вывода данных.

Для организации постраничного вывода рекомендуется последовательно выбирать данные, отсортированные по первичному ключу, ограничивая количество строк ключевым словом LIMIT.

Запрос, представленный в листинге 1, демонстрирует рекомендованный способ организации постраничного вывода.

{% note info %}

`$lastCity, $lastNumber` - значения первичного ключа, полученные в результате предыдущего запроса.

{% endnote %}

<small>Листинг 1 — запрос для организации постраничного вывода</small>

```sql
--  Table `schools`:
-- ┌─────────┬─────────┬─────┐
-- | Name    | Type    | Key |
-- ├─────────┼─────────┼─────┤
-- | city    | Utf8?   | K0  |
-- | number  | Uint32? | K1  |
-- | address | Utf8?   |     |
-- └─────────┴─────────┴─────┘

DECLARE $limit AS Uint64;
DECLARE $lastCity AS Utf8;
DECLARE $lastNumber AS Uint32;

$part1 = (
    SELECT * FROM schools
    WHERE city = $lastCity AND number > $lastNumber
    ORDER BY city, number LIMIT $limit
);

$part2 = (
    SELECT * FROM schools
    WHERE city > $lastCity
    ORDER BY city, number LIMIT $limit
);

$union = (
    SELECT * FROM $part1
    UNION ALL
    SELECT * FROM $part2
);

SELECT * FROM $union
ORDER BY city, number LIMIT $limit;
```

{% note warning "Значение NULL в ключевой колонке" %}

В {{ ydb-short-name }} все колонки, включая ключевые, могут иметь значение NULL. Несмотря на это использование NULL в качестве значений в ключевых колонках крайне не рекомендуется, так как по SQL стандарту NULL нельзя сравнивать. Как следствие, лаконичные SQL конструкции с простыми операторами сравнения будут работать некорректно. Вместо них придется использовать громоздкие конструкции с IS NULL/IS NOT NULL выражениями.

{% endnote %}

## Примеры реализации постраничного вывода

{% if oss %}
* [C++](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/pagination)
{% endif %}
* [Java](https://github.com/yandex-cloud/ydb-java-sdk/tree/master/examples/ydb-cookbook/src/main/java/com/yandex/ydb/examples/pagination)
* [Python](https://github.com/ydb-platform/ydb-python-sdk/tree/main/examples/pagination)
* [Go](https://github.com/ydb-platform/ydb-go-examples/tree/master/pagination)
