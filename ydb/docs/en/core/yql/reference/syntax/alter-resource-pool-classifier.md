# ALTER RESOURCE POOL CLASSIFIER

`ALTER RESOURCE POOL CLASSIFIER` changes the definition of a [resource pool classifier](../../../concepts/glossary.md#resource-pool-classifier).

## Syntax

### Changing parameters

The syntax for changing any resource pool classifier parameter is as follows:

```yql
ALTER RESOURCE POOL CLASSIFIER <name> SET (<key> = <value>);
```

`<key>` is the parameter name, `<value>` is its new value.

For example, the following command changes the user to which the rule applies:

```yql
ALTER RESOURCE POOL CLASSIFIER olap_classifier SET (MEMBER_NAME = "user2@domain");
```

### Resetting parameters

The command to reset a resource pool classifier parameter is as follows:

```yql
ALTER RESOURCE POOL CLASSIFIER <name> RESET (<key>);
```

`<key>` is the parameter name.

For example, the following command resets the `MEMBER_NAME` setting:

```yql
ALTER RESOURCE POOL CLASSIFIER olap_classifier RESET (MEMBER_NAME);
```

## Permissions

The `ALL` [permission](grant.md#permissions-list) on the database is required. Example of granting it:

```yql
GRANT 'ALL' ON `/my_db` TO `user1@domain`;
```

## Parameters

* `RANK` (Int64) — Optional field that defines the order in which resource pool classifiers are chosen. If omitted, the maximum existing `RANK` is taken and 1000 is added. Allowed values: a unique number in the range $[0, 2^{63}-1]$.
* `RESOURCE_POOL` (String) — Required field: name of the resource pool to which queries matching the classifier criteria are sent.
* `MEMBER_NAME` (String) — Optional field specifying which user or group is routed to the given resource pool. If omitted, the classifier ignores `MEMBER_NAME` and classification uses other criteria.

## See also

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](create-resource-pool-classifier.md)
* [{#T}](drop-resource-pool-classifier.md)
