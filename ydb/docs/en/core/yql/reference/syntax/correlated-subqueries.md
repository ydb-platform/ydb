# Correlated subqueries, EXISTS, and NOT EXISTS

A correlated subquery refers to a column or table alias from an outer query.
Each subquery in YQL has its own scope and cannot refer to columns or table
aliases from an outer query. Therefore, YQL does not support correlated
subqueries.

## Support matrix {#support-matrix}

| SQL pattern | YQL support | Use instead |
| --- | --- | --- |
| An uncorrelated subquery in `FROM` or `IN` | Supported | Use the subquery directly |
| An uncorrelated `EXISTS (SELECT ...)` | Supported; returns `true` if the subquery contains at least one row | Use directly when the condition does not depend on an outer row |
| An uncorrelated `NOT EXISTS (SELECT ...)` | Supported; returns `true` if the subquery is empty | Use directly when the condition does not depend on an outer row |
| `EXISTS` referring to an outer column | Not supported | `LEFT SEMI JOIN`, or `INNER JOIN` with unique right-side keys |
| `NOT EXISTS` referring to an outer column | Not supported | `LEFT ONLY JOIN` |
| A correlated scalar or aggregate subquery | Not supported | Precompute the result and use `JOIN` |

## EXISTS {#exists}

An uncorrelated `EXISTS` checks whether its subquery contains at least one row.
Its result does not depend on a row from an outer query. For example, the
following query returns every client if the `orders` table is non-empty and no
clients otherwise:

```yql
SELECT client_id, name
FROM clients
WHERE EXISTS (
    SELECT 1
    FROM orders
);
```

The following common SQL query is not supported because the inner query refers
to the outer alias `c`:

```yql
-- Not supported in YQL.
SELECT c.client_id, c.name
FROM clients AS c
WHERE EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.client_id = c.client_id
);
```

Use `LEFT SEMI JOIN` to return a client only when at least one matching order
exists:

```yql
SELECT c.client_id, c.name
FROM clients AS c
LEFT SEMI JOIN orders AS o
ON o.client_id = c.client_id;
```

`LEFT SEMI JOIN` returns columns from the left side only. Several matching rows
on the right side do not duplicate a row from the left side, so this rewrite
preserves the existence-check semantics.

Alternatively, use `INNER JOIN` after deduplicating the matching keys on the
right side:

```yql
$order_clients = (
    SELECT client_id
    FROM orders
    GROUP BY client_id
);

SELECT c.client_id, c.name
FROM clients AS c
INNER JOIN $order_clients AS o
ON o.client_id = c.client_id;
```

The right side contains at most one row for each key, so the join does not
duplicate rows from the left side. Applying `DISTINCT` to the left-side columns
is unnecessary and can collapse equal rows from the outer input.

For a single non-optional key, `IN` is another possible rewrite:

```yql
SELECT c.client_id, c.name
FROM clients AS c
WHERE c.client_id IN (
    SELECT o.client_id
    FROM orders AS o
);
```

## NOT EXISTS {#not-exists}

An uncorrelated `NOT EXISTS` returns the inverse of `EXISTS`: `true` when its
subquery is empty and `false` when the subquery contains at least one row. Its
result also does not depend on a row from an outer query.

Use `LEFT ONLY JOIN` to return a client only when no matching order exists:

```yql
SELECT c.client_id, c.name
FROM clients AS c
LEFT ONLY JOIN orders AS o
ON o.client_id = c.client_id;
```

## Correlated aggregate subqueries {#correlated-aggregate}

Precompute the aggregate and join it to the outer table:

```yql
$last_order = (
    SELECT client_id, MAX(created_at) AS last_order_at
    FROM orders
    GROUP BY client_id
);

SELECT c.client_id, o.last_order_at
FROM clients AS c
LEFT JOIN $last_order AS o
ON o.client_id = c.client_id;
```

## See also

- [JOIN](select/join.md)
- [Expressions: IN](expressions.md#in)
- [Known limitations](../../../analyst/limitations.md)
