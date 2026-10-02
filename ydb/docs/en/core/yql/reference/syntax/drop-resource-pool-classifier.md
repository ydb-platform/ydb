# DROP RESOURCE POOL CLASSIFIER

`DROP RESOURCE POOL CLASSIFIER` removes a [resource pool classifier](../../../concepts/glossary.md#resource-pool-classifier).

## Syntax

```yql
DROP RESOURCE POOL CLASSIFIER <name>
```

### Parameters

* `name` — name of the resource pool classifier to drop.

## Permissions

The `ALL` [permission](grant.md#permissions-list) on the database is required. Example:

```yql
GRANT 'ALL' ON `/my_db` TO `user1@domain`;
```

## Examples

The following removes the classifier named `olap_classifier`:

```yql
DROP RESOURCE POOL CLASSIFIER olap_classifier;
```

## See also

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](create-resource-pool-classifier.md)
* [{#T}](alter-resource-pool-classifier.md)
