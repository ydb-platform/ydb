# Права

## Основные понятия

Права в {{ ydb-short-name }} выдаются на объекты доступа. У каждого такого объекта есть список разрешений — ACL (Access Control List) — он хранит все предоставленные пользователям и группам права на объект. Объектом может быть директория, содержащая другие поддиректории или непосредственно объекты, не имеющие других вложенных объектов, — таблицы, топики, представления и не только.
В дальнейшем, если внутри директории появляются другие объекты, они по умолчанию наследуют все права, выданные на родительскую директорию.

{% note info %}

Права привязаны не пользователю, а к объекту доступа.

{% endnote %}

{% include [x](../yql/reference/yql-core/syntax/_includes/permissions/permissions_list.md) %}

## Управление правами

### Управление правами с помощью YQL

Для управления правами служат следующие YQL-команды:

* [{#T}](../yql/reference/syntax/grant.md).
* [{#T}](../yql/reference/syntax/revoke.md).

В качестве имен прав доступа в этих командах можно использовать специфичные для {{ ydb-short-name }} права или соответствующие им ключевые слова.

Пример использования c ключевым словом YQL:

```yql
GRANT CREATE DIRECTORY ON `/Root/db1` TO testuser
```

Пример использования c именем {{ ydb-short-name }} права:

```yql
GRANT "ydb.granular.create_directory" ON `/Root/db1` TO testuser
```

### Управление правами с помощью CLI

Для управления правами служат следующие CLI-команды:

* [chown](../reference/ydb-cli/commands/scheme-permissions.md#chown)
* [grant](../reference/ydb-cli/commands/scheme-permissions.md#grant-revoke)
* [revoke](../reference/ydb-cli/commands/scheme-permissions.md#grant-revoke)
* [set](../reference/ydb-cli/commands/scheme-permissions.md#set)
* [clear](../reference/ydb-cli/commands/scheme-permissions.md#clear)
* [list](../reference/ydb-cli/commands/scheme-permissions.md#list)

В CLI используется только стиль из столбца "{{ ydb-short-name }} право".
Например:

```bash
{{ ydb-short-name }} scheme permissions grant -p "ydb.granular.create_directory" `/Root/db1` testuser
```

### Просмотр прав с помощью CLI

Просматривать ACL объекта доступа можно с помощью CLI команды [`describe`](../reference/ydb-cli/commands/scheme-describe.md).
