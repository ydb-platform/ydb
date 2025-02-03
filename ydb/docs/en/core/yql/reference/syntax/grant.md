# GRANT

The `GRANT` command allows setting access rights to schema objects for a user or group of users.

Syntax:

```yql
GRANT {{permission_name} [, ...] | ALL [PRIVILEGES]} ON {path_to_scheme_object [, ...]} TO {role_name [, ...]} [WITH GRANT OPTION]
```

* `permission_name` - the name of the access right to schema objects that needs to be assigned.
* `path_to_scheme_object` - the path to the schema object to which rights are granted.
* `role_name` - the name of the user or group for which rights to the schema object are granted.

`WITH GRANT OPTION` - using this clause gives the user or group the right to manage access rights - to grant or revoke specific rights. The clause functions similarly to granting the `"ydb.access.grant"` right or `GRANT`.
A subject with the `ydb.access.grant` right cannot grant rights broader than they themselves have on the access object `path_to_scheme_object`.

{% include [x](../_includes/permissions_list.md) %}

## Examples

* Assign the `ydb.generic.read` right to the table `/shop_db/orders` for the user `user1`:

  ```yql
  GRANT 'ydb.generic.read' ON `/shop_db/orders` TO user1;
  ```

  The same command, using the keyword:

  ```yql
  GRANT SELECT ON `/shop_db/orders` TO user1;
  ```

* Assign the rights `ydb.database.connect` and `ydb.generic.list` to the root of the database `/shop_db` for user `user2` and group `group1`:

  ```yql
  GRANT LIST, CONNECT ON `/shop_db` TO user2, group1;
  ```

* Assign the `ydb.generic.use` right to the tables `/shop_db/orders` and `/shop_db/sellers` for users `user1@domain` and `user2@domain`:

  ```yql
  GRANT 'ydb.generic.use' ON `/shop_db/orders`, `/shop_db/sellers` TO `user1@domain`, `user2@domain`;
  ```

* Grant all rights to the table `/shop_db/sellers` for the user `admin_user`:

  ```yql
  GRANT ALL ON `/shop_db/sellers` TO admin_user;
  ```
