# CREATE GROUP

Creates a [group](../../../concepts/glossary.md#access-group) with the specified name. Optionally, you can specify a list of [users](../../../concepts/glossary.md#access-user) to add to the group.

## Syntax

```yql
CREATE GROUP group_name [ WITH USER user_name [ , user_name [ ... ]] [ , ] ]
```

### Parameters

* `group_name`: The name of the group. It may contain lowercase Latin letters and digits.
* `user_name`: The name of the user who will become a member of the group after its creation. It may contain lowercase Latin letters and digits.

## Examples

```yql
CREATE GROUP group1;
```

```yql
CREATE GROUP group2 WITH USER user1;
```

```yql
CREATE GROUP group3 WITH USER user1, user2,;
```

```yql
CREATE GROUP group4 WITH USER user1, user3, user2;
```
