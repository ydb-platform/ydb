# CREATE BACKUP COLLECTION

Выражение `CREATE BACKUP COLLECTION` создает [коллекцию резервных копий](../../../concepts/backup-collections.md).

```yql
CREATE BACKUP COLLECTION collection_name (
    TABLE table_name [, TABLE another_table_name ...]
) WITH (option = value [, ...]);
```

## Параметры

* `collection_name` — имя создаваемой коллекции резервных копий.
* `table_name` — полный путь к таблице для включения в коллекцию. Можно указать несколько таблиц.
* Опции:

  * `STORAGE` — бэкенд хранения для резервных копий. В настоящее время поддерживается только `'cluster'`.
  * `INCREMENTAL_BACKUP_ENABLED` — включение-выключение поддержки инкрементальных резервных копий. Установите в `'true'` для включения инкрементальных резервных копий, `'false'` — только для полных резервных копий.

{% note info %}

При выборе имени для коллекции резервных копий учитывайте общие [правила именования объектов схемы](../../../concepts/datamodel/cluster-namespace.md#object-naming-rules)

{% endnote %}

## Примеры

Создание коллекции резервных копий с одной таблицей:

```yql
CREATE BACKUP COLLECTION daily_backups (
    TABLE orders
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);
```

Создание коллекции резервных копий с несколькими таблицами:

```yql
CREATE BACKUP COLLECTION production_backups (
    TABLE orders,
    TABLE products,
    TABLE customers
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);
```

## См. также

* [Коллекции резервных копий](../../../concepts/backup-collections.md).
* [BACKUP](backup.md).
* [RESTORE](restore-backup-collection.md).
* [DROP BACKUP COLLECTION](drop-backup-collection.md).
