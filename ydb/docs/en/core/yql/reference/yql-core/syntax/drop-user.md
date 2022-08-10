# DROP USER

Deletes the specified user. You can list multiple users under one operator.

Syntax:

```yql
DROP USER [ IF EXISTS ] user_name [, ...]
```

* `IF EXISTS`: Suppress an error if the user doesn't exist.
* `user_name`: The name of the user to be deleted.
