# CREATE SECRET

Команда `CREATE SECRET` создаёт [секрет](../../../concepts/datamodel/secrets.md).

Синтаксис:

```sql
CREATE [OR REPLACE] SECRET [IF NOT EXISTS] secret_name
WITH (option = value[, ...])
```

* `OR REPLACE` — если секрет с таким именем уже существует, он будет заменён новым определением.
* `IF NOT EXISTS` — команда не возвращает ошибку, если секрет с таким именем уже существует; существующий объект останется без изменений.
* `secret_name` — имя создаваемого секрета.
* `option` — опция команды:
  * `value` — строка со значением секрета.
  * `inherit_permissions` — опция, при включении которой [права](grant.md) на секрет наследуются от директории, в которой секрет создаётся. При отключении опции от директории наследуется только [право](grant.md#permissions-list) `DESCRIBE SCHEMA`. Владелец секрета получает все возможные права на него в любом случае. По умолчанию — `False`.

{% note warning %}

Конструкции `OR REPLACE` и `IF NOT EXISTS` нельзя использовать одновременно.

{% endnote %}

## Разрешения

Для создания секрета требуется [право](grant.md#permissions-list) `CREATE TABLE`.

При использовании `CREATE OR REPLACE SECRET` для существующего секрета требуется [право](grant.md#permissions-list) `ALTER SCHEMA` на секрет, так как эта форма `CREATE` изменяет секрет. Если секрет не существует, достаточно права `CREATE TABLE` на родительскую директорию.

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

Создать секрет с именем `secret_name`, только если он не существует; если он существует, существующий секрет останется без изменений:

```sql
CREATE SECRET IF NOT EXISTS secret_name WITH (value = "secret_value");
```

Создать или заменить секрет с именем `secret_name`; если он существует, он будет заменён новым определением:

```sql
CREATE OR REPLACE SECRET secret_name WITH (value = "secret_value");
```

## См. также

* [ALTER SECRET](alter-secret.md)
* [DROP SECRET](drop-secret.md)
