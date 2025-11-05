# CREATE BACKUP COLLECTION

Выражение `CREATE BACKUP COLLECTION` создает [коллекцию резервных копий](../../../concepts/backup-collections.md) - именованный набор скоординированных резервных копий для выбранных объектов базы данных.

```yql
CREATE BACKUP COLLECTION `имя_коллекции` (
    TABLE `путь_к_таблице` [, TABLE `путь_к_таблице` ...]
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);
```

## Параметры

* `имя_коллекции`: Имя создаваемой коллекции резервных копий.
* `путь_к_таблице`: Полный путь к таблице для включения в коллекцию. Можно указать несколько таблиц.
* `STORAGE`: Бэкенд хранения для резервных копий. В настоящее время поддерживается только `'cluster'`.
* `INCREMENTAL_BACKUP_ENABLED`: Включить поддержку инкрементальных резервных копий. Установите в `'true'` для включения инкрементальных резервных копий, `'false'` только для полных резервных копий.

{% note info %}

При выборе имени для коллекции резервных копий учитывайте общие [правила именования объектов схемы](../../../concepts/datamodel/cluster-namespace.md#object-naming-rules)

{% endnote %}

## Примеры

Создание коллекции резервных копий с одной таблицей:

```yql
CREATE BACKUP COLLECTION `daily_backups` (
    TABLE `/Root/shop/orders`
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);
```

Создание коллекции резервных копий с несколькими таблицами:

```yql
CREATE BACKUP COLLECTION `production_backups` (
    TABLE `/Root/shop/orders`,
    TABLE `/Root/shop/products`,
    TABLE `/Root/shop/customers`
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);
```

## См. также

* [Концепции коллекций резервных копий](../../../concepts/backup-collections.md)
* [BACKUP](backup.md)
* [RESTORE](restore-backup-collection.md)
* [DROP BACKUP COLLECTION](drop-backup-collection.md)
