# Paginated output

This section provides recommendations for organizing paginated data output.

To organize paginated output, we recommend selecting data sorted by primary key sequentially, limiting the number of rows with the LIMIT keyword.

The query in listing 1 demonstrates the recommended way to organize paginated output.

{% note info %}

`$lastCity, $lastNumber`: Primary key values obtained from the previous query.

{% endnote %}

<small>Listing 1: Query for organizing paginated output</small>

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

{% note warning "NULL value in key column" %}

In {{ ydb-short-name }}, all columns, including key ones, may have a NULL value. Despite this, using NULL as key column values is highly discouraged, since the SQL standard doesn't allow NULL to be compared. As a result, concise SQL statements with simple comparison operators won't work correctly. Instead, you'll have to use cumbersome statements with IS NULL/IS NOT NULL expressions.

{% endnote %}

## Examples of paginated output implementation

{% if oss %}

* [C++](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/pagination)
{% endif %}
* [Java](https://github.com/yandex-cloud/ydb-java-sdk/tree/master/examples/ydb-cookbook/src/main/java/com/yandex/ydb/examples/pagination)
* [Python](https://github.com/ydb-platform/ydb-python-sdk/tree/main/examples/pagination)
* [Go](https://github.com/ydb-platform/ydb-go-examples/tree/master/pagination)

