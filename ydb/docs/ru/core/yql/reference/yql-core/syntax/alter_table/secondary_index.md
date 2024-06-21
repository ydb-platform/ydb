# Добавление, удаление и переименование вторичного индекса

{% if backend_name == "YDB" %}

{% include [OLAP_not_allow_note](../../../../_includes/not_allow_for_olap_note.md) %}

{% endif %}

### Добавление индекса

```ADD INDEX``` — добавляет индекс с указанным именем и типом для заданного набора колонок в {% if backend_name == "YDB" %}строковых таблицах.{% else %}таблицах.{% endif %} Приведенный ниже код добавит глобальный индекс с именем ```title_index``` для колонки ```title```.

```sql
ALTER TABLE `series` ADD INDEX `title_index` GLOBAL ON (`title`);
```

Могут быть указаны все параметры индекса, описанные в команде [`CREATE TABLE`](../create_table#secondary_index)

### Удаление индекса

```DROP INDEX``` — удаляет индекс с указанным именем. Приведенный ниже код удалит индекс с именем ```title_index```.

```sql
ALTER TABLE `series` DROP INDEX `title_index`;
```

Также добавить или удалить вторичный индекс у строковой таблицы можно с помощью команды [table index](https://ydb.tech/ru/docs/reference/ydb-cli/commands/secondary_index) {{ ydb-short-name }} CLI.

### Переименование вторичного индекса {#rename-secondary-index}

`RENAME INDEX` — переименовывает индекс с указанным именем. Если индекс с новым именем существует, будет возвращена ошибка.

{% if backend_name == YDB %}

Возможность атомарной замены индекса под нагрузкой поддерживается командой [{{ ydb-cli }} table index rename](../../../../reference/ydb-cli/commands/secondary_index.md#rename) {{ ydb-short-name }} CLI и специализированными методами {{ ydb-short-name }} SDK.

{% endif %}

Пример переименования индекса:

```sql
ALTER TABLE `series` RENAME INDEX `title_index` TO `title_index_new`;
```