# DROP BACKUP COLLECTION

Выражение `DROP BACKUP COLLECTION` удаляет [коллекцию резервных копий](../../../concepts/backup-collections.md) и все содержащиеся в ней резервные копии.

```yql
DROP BACKUP COLLECTION collection_name;
```

## Параметры

* `collection_name` — имя удаляемой коллекции резервных копий.

{% note warning %}

Эта операция безвозвратно удаляет коллекцию резервных копий и все содержащиеся в ней резервные копии. Это действие не может быть отменено.

{% endnote %}

## Примеры

Удаление коллекции резервных копий:

```yql
DROP BACKUP COLLECTION old_backups;
```

## См. также

* [Коллекции резервных копий](../../../concepts/backup-collections.md).
* [CREATE BACKUP COLLECTION](create-backup-collection.md).
* [BACKUP](backup.md).
* [RESTORE](restore-backup-collection.md).
