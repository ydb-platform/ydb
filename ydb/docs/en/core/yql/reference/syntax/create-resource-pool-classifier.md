# CREATE RESOURCE POOL CLASSIFIER

`CREATE RESOURCE POOL CLASSIFIER` creates a [resource pool classifier](../../../concepts/glossary.md#resource-pool-classifier).

## Syntax

```yql
CREATE RESOURCE POOL CLASSIFIER <name>
WITH ( <parameter_name> [= <parameter_value>] [, ... ] )
```

- `name` — name of the resource pool classifier to create. Must be unique and must not contain characters forbidden for schema objects.
- `WITH ( <parameter_name> [= <parameter_value>] [, ... ] )` — parameters that define classifier behavior.

### Parameters

* `RANK` (Int64) — Optional: order in which classifiers are evaluated. If omitted, the maximum existing `RANK` plus 1000 is used. Allowed values: a unique number in $[0, 2^{63}-1]$.
* `RESOURCE_POOL` (String) — Required: name of the resource pool for queries that match the classifier.
* `MEMBER_NAME` (String) — Optional: user or group routed to that pool. If omitted, the classifier ignores `MEMBER_NAME` and uses other criteria.

## Notes {#remarks}

If `RANK` is omitted in the DDL, the default is $RANK = MAX(existing\_ranks) + 1000$. All `RANK` values must be unique so pool choice is deterministic when rules conflict. This allows inserting new classifiers between existing ones.

A classifier may reference a non-existent pool or a pool the user cannot access; such classifiers are skipped.

Classifier count limits are described on the [limits](../../../concepts/limits-ydb.md#resource_pool) page.

## Permissions

The `ALL` [permission](grant.md#permissions-list) on the database is required.

Example:

```yql
GRANT 'ALL' ON `/my_db` TO `user1@domain`;
```

## Examples {#examples}

```yql
CREATE RESOURCE POOL CLASSIFIER olap_classifier WITH (
    RANK=1000,
    RESOURCE_POOL="olap",
    MEMBER_NAME="user1@domain"
)
```

This creates classifier `olap_classifier` that sends queries from `user1@domain` to pool `olap`. Other users use the `default` pool unless other classifiers apply.

## See also

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](alter-resource-pool-classifier.md)
* [{#T}](drop-resource-pool-classifier.md)
