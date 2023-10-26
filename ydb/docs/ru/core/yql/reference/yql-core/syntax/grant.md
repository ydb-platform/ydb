# GRANT

Команда `GRANT` позволяет установить права доступа к объектам схемы для пользователя или группы пользователей.

Синтаксис:

```yql
GRANT {{permission_name} [, ...] | ALL [PRIVILEGES]} ON {path_to_scheme_object [, ...]} TO {role_name [, ...]} [WITH GRANT OPTION]
```

* `permission_name` - имя права доступа к объектам схемы, которое нужно назначить.
* `path_to_scheme_object` - путь до объекта схемы, на который выдаются права.
* `role_name` - имя пользователя или группы, для которого выдаются права на объект схемы.

`WITH GRANT OPTION` - использование этой конструкции наделяет пользователя или группу пользователей правом управлять правами доступа - назначать или отзывать определенные права. Конструкция имееет функцианальность аналогичную выдаче права `"ydb.access.grant"` или `GRANT`.
Субъект, обладающий правом `ydb.access.grant`, не может выдавать права шире, чем обладает сам.

{% include [x](_includes/permissions/permissions_list.md) %}

## Примеры
* Назначить право `ydb.generic.read` на таблицу `/shop_db/orders` для пользователя `user1`:
  ```
  GRANT 'ydb.generic.read' ON `/shop_db/orders` TO user1;
  ```
  Та же команда, с использованием ключевого слова
  ```
  GRANT SELECT ON `/shop_db/orders` TO user1;
  ```

* Назначить права `ydb.database.connect`, `ydb.generic.list` на корень базы `/shop_db` для пользователя `user2` и группы `group1`:
  ```
  GRANT LIST, CONNECT ON `/shop_db` TO user2, group1;
  ```

* Назначить право `ydb.generic.use` на таблицы `/shop_db/orders` и `/shop_db/sellers` для пользователей `user1@domain`, `user2@domain`:
  ```
  GRANT 'ydb.generic.use' ON `/shop_db/orders`, `/shop_db/sellers` TO `user1@domain`, `user2@domain`;
  ```

* Назначить все права на таблицу `/shop_db/sellers` для пользователя `admin_user`:
  ```
  GRANT ALL ON `/shop_db/sellers` TO admin_user;
  ```
