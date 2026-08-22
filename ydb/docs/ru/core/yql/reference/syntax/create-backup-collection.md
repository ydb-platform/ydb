# CREATE BACKUP COLLECTION

Выражение `CREATE BACKUP COLLECTION` создает [коллекцию резервных копий](../../../concepts/datamodel/backup-collection.md).

```yql
CREATE BACKUP COLLECTION collection_name (
    TABLE table_name [, TABLE another_table_name ...]
) WITH (option = value [, ...]);
```

## Параметры

* `collection_name` — имя создаваемой коллекции резервных копий.
* `table_name` — полный путь к таблице для включения в коллекцию. Можно указать несколько таблиц.
* Опции:

  * `STORAGE` — тип хранилища для резервных копий. Поддерживаемые варианты:
    * `'cluster'` — хранение в кластере {{ ydb-short-name }}.
  * `INCREMENTAL_BACKUP_ENABLED` — включает или отключает поддержку инкрементальных резервных копий. Установите в `'true'` для включения инкрементальных резервных копий, `'false'` — только для полных резервных копий.

{% note info %}

При выборе имени коллекции резервных копий учитывайте общие [правила именования схемных объектов](../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

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

* [Коллекции резервных копий](../../../concepts/datamodel/backup-collection.md).
* [BACKUP](backup.md).
* [RESTORE](restore-backup-collection.md).
* [DROP BACKUP COLLECTION](drop-backup-collection.md).
