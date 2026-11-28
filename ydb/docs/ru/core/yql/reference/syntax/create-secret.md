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
  * `SOURCE` — источник значения [внешнего секрета](../../../concepts/datamodel/secrets.md#types). Если не указан, создаётся секрет со значением из опции `value`. Поддерживается значение `IAM_DELEGATION` — [секрет с делегированием IAM](../../../concepts/datamodel/iam-delegation-secrets.md), доступный только в {{ yandex-cloud }}. Для внешнего секрета опция `value` не задаётся. Каждый источник принимает свой набор опций; при указании опции, которую источник не принимает, команда завершается ошибкой со списком допустимых опций.
  * `SERVICE_ACCOUNT_ID` — только для `SOURCE="IAM_DELEGATION"`: идентификатор сервисного аккаунта, от имени которого будет действовать {{ ydb-short-name }}. Обязательная опция.
  * `RESOURCE` — только для `SOURCE="IAM_DELEGATION"`: идентификатор облака, которому принадлежит сервисный аккаунт. Если не указан, определяется по сервисному аккаунту, а при невозможности — берётся облако текущей базы данных с предупреждением в ответе на запрос.

{% note warning %}

Конструкции `OR REPLACE` и `IF NOT EXISTS` нельзя использовать одновременно.

{% endnote %}

## Разрешения

Для создания секрета требуется [право](grant.md#permissions-list) `CREATE TABLE`.

При использовании `CREATE OR REPLACE SECRET` для существующего секрета требуется [право](grant.md#permissions-list) `ALTER SCHEMA` на секрет, так как эта форма `CREATE` изменяет секрет. Если секрет не существует, достаточно права `CREATE TABLE` на родительскую директорию.

Для создания секрета с делегированием IAM дополнительно требуется, чтобы запрос выполнял субъект {{ yandex-cloud }}, аутентифицированный через IAM, имеющий роль `iam.serviceAccounts.user` на указанный сервисный аккаунт. Делегирование настраивается в IAM от имени этого пользователя; если IAM отказывает, секрет не создаётся.

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

Создать [секрет с делегированием IAM](../../../concepts/datamodel/iam-delegation-secrets.md): вместо значения в нём сохраняется разрешение действовать от имени сервисного аккаунта `aje8k2vqp3n1rd7cmf4t`, а при чтении секрет отдаёт действующий IAM-токен этого аккаунта:

```sql
CREATE SECRET events_sa WITH (SOURCE="IAM_DELEGATION", SERVICE_ACCOUNT_ID="aje8k2vqp3n1rd7cmf4t");
```

То же с явным указанием облака, которому принадлежит сервисный аккаунт:

```sql
CREATE SECRET events_sa WITH (SOURCE="IAM_DELEGATION", SERVICE_ACCOUNT_ID="aje8k2vqp3n1rd7cmf4t", RESOURCE="b1gxxxxxxxxx");
```

## См. также

* [ALTER SECRET](alter-secret.md)
* [DROP SECRET](drop-secret.md)
* [{#T}](../../../concepts/datamodel/iam-delegation-secrets.md)
