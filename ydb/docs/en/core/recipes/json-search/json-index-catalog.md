# Catalog with nested attributes

This recipe shows how to use a [JSON index](../../dev/json-indexes.md) to speed up access to a product catalog where product characteristics are stored as a JSON document with an arbitrary set of fields. This schema is convenient when:

* the set of product attributes is unknown in advance or differs for different categories;
* adding new attributes via `ALTER TABLE ADD COLUMN` is undesirable;
* you need to efficiently filter products by various combinations of attributes.

A JSON index enables efficient retrieval by any path and value inside a JSON document without a full table scan.

## Create a table and an index


```yql
CREATE TABLE products (
    sku_id Uint64,
    attrs JsonDocument,
    PRIMARY KEY (sku_id),
    INDEX attrs_json_idx GLOBAL USING json ON (attrs)
);
```


In this schema:

* `sku_id` — numeric product identifier.
* `attrs` — product attributes in `JsonDocument` format. Storing as `JsonDocument` saves space and speeds up deserialization compared to `Json`.
* `attrs_json_idx` — JSON index on column `attrs`. It is updated synchronously with the main table.

## Load test data


```yql
UPSERT INTO products (sku_id, attrs) VALUES
    (10, JsonDocument(@@{
        "brand": "ACME",
        "price": 49.90,
        "category": "tools",
        "warehouses": [{"id": 1, "stock": 12}, {"id": 2, "stock": 0}]
    }@@)),
    (11, JsonDocument(@@{
        "brand": "ACME",
        "price": 199.00,
        "category": "electronics",
        "warehouses": [{"id": 1, "stock": 3}]
    }@@)),
    (12, JsonDocument(@@{
        "brand": "Globex",
        "price": 25.00,
        "category": "tools",
        "warehouses": [{"id": 2, "stock": 0}]
    }@@));
```


## Filter by brand and price range


```yql
SELECT sku_id, attrs
FROM products VIEW attrs_json_idx
WHERE JSON_VALUE(attrs, '$.brand' RETURNING Utf8) = "ACME"u
  AND JSON_VALUE(attrs, '$.price' RETURNING Double) BETWEEN 10.0 AND 100.0;
```


What happens:

* For the `$.brand = "ACME"` condition, the index stores a token of "path + value" — this provides an exact match and maximum selectivity.
* For the `BETWEEN 10.0 AND 100.0` condition, the index stores only the path token `$.price`. This narrows down the set of rows to those where the `price` field is present, after which the query execution engine checks the range with an exact comparison (post-filter).
* The conditions are combined with `AND` — this allows the index to check both fragments at once, resulting in minimal index reads.

Result:


```text
sku_id attrs
10     {"brand":"ACME","price":49.9,...}
```


## Search in a nested array

JsonPath supports accessing array elements and filters inside a path. For example, you can find products that have stock in at least one warehouse:


```yql
SELECT sku_id
FROM products VIEW attrs_json_idx
WHERE JSON_EXISTS(attrs, '$.warehouses ? (@.stock > 0)');
```


The index search uses the path token `$.warehouses.stock`, and the `@.stock > 0` condition is checked by a post-filter on each found record. This is efficient when the corresponding field is present in a relatively small portion of documents.

Result:


```text
sku_id
10
11
```


## Search by category and attribute presence

Conditions can be combined with any `AND` and `OR` operators. Search for products in the `tools` category that have the `price` field specified:


```yql
SELECT sku_id
FROM products VIEW attrs_json_idx
WHERE JSON_VALUE(attrs, '$.category' RETURNING Utf8) = "tools"u
  AND JSON_EXISTS(attrs, '$.price');
```


Result:


```text
sku_id
10
12
```


## More details

* [Supported JSON index predicates](../../dev/json-indexes.md#predicates) — full rules for which expressions are indexed.
* [AND and OR handling](../../dev/json-indexes.md#predicates) — nuances of combining conditions, including with non-indexable predicates.
* [{#T}](json-index-parameters.md) — parameterized versions of the same queries.
