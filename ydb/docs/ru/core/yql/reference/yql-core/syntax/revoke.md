# REVOKE

Команда `REVOKE` позволяет отозвать права доступа к объектам схемы для пользователей или групп пользователей.

Синтаксис:

```yql
REVOKE [GRANT OPTION FOR] {{permission_name} [, ...] | ALL [PRIVILEGES]} ON {path_to_scheme_object [, ...]} FROM {role_name [, ...]}
```

* `permission_name` - имя права доступа к объектам схемы, которое нужно отозвать.
* `path_to_scheme_object` - путь до объекта схемы, с которого отзываются права.
* `role_name` - имя пользователя или группы, для которого отзываются права на объект схемы.

`GRANT OPTION FOR` - использование этой конструкции отзывает у пользователя или группы право управлять правами доступа. Все ранее выданные этим пользователем права остаются в силе. Конструкция имеет функцианальность аналогичную отзыву права `"ydb.access.grant"` или `GRANT`.

{% include [x](_includes/permissions/permissions_list.md) %}

## Примеры
* Отозвать право `ydb.generic.read` на таблицу `/shop_db/orders` у пользователя `user1`:
  ```
  REVOKE 'ydb.generic.read' ON `/shop_db/orders` FROM user1;
  ```
  Та же команда, с использованием ключевого слова
  ```
  REVOKE SELECT ON `/shop_db/orders` FROM user1;
  ```

* Отозвать права `ydb.database.connect`, `ydb.generic.list` на корень базы `/shop_db` у пользователя `user2` и группы `group1`:
  ```
  REVOKE LIST, CONNECT ON `/shop_db` FROM user2, group1;
  ```

* Отозвать право `ydb.generic.use` на таблицы `/shop_db/orders` и `/shop_db/sellers` у пользователей `user1@domain`, `user2@domain`:
  ```
  REVOKE 'ydb.generic.use' ON `/shop_db/orders`, `/shop_db/sellers` FROM `user1@domain`, `user2@domain`;
  ```

* Отозвать все права на таблицу `/shop_db/sellers` для пользователя `user`:
  ```
  REVOKE ALL ON `/shop_db/sellers` FROM user;
  ```
