# DROP GROUP

Deletes the specified group. You can list multiple groups under one operator.

Syntax:

```yql
DROP GROUP [ IF EXISTS ] group_name [, ...]
```

* `IF EXISTS`: Suppress an error if the group doesn't exist.
* `group_name`: The name of the group to be deleted.
