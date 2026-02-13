# CREATE SECRET

Команда `CREATE SECRET` создаёт [секрет](../../../concepts/datamodel/secrets.md).

Синтаксис:

```sql
CREATE SECRET secret_name
WITH (option = value[, ...])
```

* `secret_name` — имя создаваемого секрета.
* `option` — опция команды:
  * `value` — строка со значением секрета.
  * `inherit_permissions` — опция, при включении которой [права](grant.md) на секрет наследуются от директории, в которой секрет создаётся. При отключении опции от директории наследуется только [право](grant.md#permissions-list) `DESCRIBE SCHEMA`. Владелец секрета получает все возможные права на него в любом случае. По умолчанию — `False`.

## Разрешения

Для создания секрета требуется [право](grant.md#permissions-list) `CREATE TABLE`.

## Примеры

Создать секрет в корне базы с именем `secret_name` и значением `secret_value`:

```sql
CREATE SECRET secret_name WITH (value = "secret_value");
```

Создать секрет в директории `dir` в корне базы с именем `secret_name` и значением `secret_value`. Если директория `dir` не существует, она будет создана:

```sql
CREATE SECRET `dir/secret_name` WITH (value = "secret_value");
```

Создать секрет в корне базы с именем `secret_name` и значением `secret_value` с правами такими же, как у родительской директории секрета:

```sql
CREATE SECRET secret_name WITH (value = "secret_value", inherit_permissions = True);
```

## См. также

* [ALTER SECRET](alter-secret.md)
* [DROP SECRET](drop-secret.md)
