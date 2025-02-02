# REVOKE

The `REVOKE` command allows revoking access rights to schema objects for users or groups of users.

Syntax:

```yql
REVOKE [GRANT OPTION FOR] {{permission_name} [, ...] | ALL [PRIVILEGES]} ON {path_to_scheme_object [, ...]} FROM {role_name [, ...]}
```

* `permission_name` - the name of the access right to schema objects that needs to be revoked.
* `path_to_scheme_object` - the path to the schema object from which rights are revoked.
* `role_name` - the name of the user or group from which rights to the schema object are revoked.

`GRANT OPTION FOR` - using this clause revokes the right to manage access rights from the user or group. All rights previously granted by this user remain in effect. The clause functions similarly to revoking the `"ydb.access.grant"` right or `GRANT`.

{% include [x](../_includes/permissions_list.md) %}

## Examples

* Revoke the `ydb.generic.read` right on the table `/shop_db/orders` from user `user1`:

  ```yql
  REVOKE 'ydb.generic.read' ON `/shop_db/orders` FROM user1;
  ```

  The same command, using the keyword:

  ```yql
  REVOKE SELECT ON `/shop_db/orders` FROM user1;
  ```

* Revoke the rights `ydb.database.connect`, `ydb.generic.list` on the root of the database `/shop_db` from user `user2` and group `group1`:

  ```yql
  REVOKE LIST, CONNECT ON `/shop_db` FROM user2, group1;
  ```

* Revoke the `ydb.generic.use` right on the tables `/shop_db/orders` and `/shop_db/sellers` from users `user1@domain` and `user2@domain`:

  ```yql
  REVOKE 'ydb.generic.use' ON `/shop_db/orders`, `/shop_db/sellers` FROM `user1@domain`, `user2@domain`;
  ```

* Revoke all rights on the table `/shop_db/sellers` from user `user`:

  ```yql
  REVOKE ALL ON `/shop_db/sellers` FROM user;
  ```
