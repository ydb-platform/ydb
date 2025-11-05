# DROP BACKUP COLLECTION

Выражение `DROP BACKUP COLLECTION` удаляет [коллекцию резервных копий](../../../concepts/backup-collections.md) и все содержащиеся в ней резервные копии.

```yql
DROP BACKUP COLLECTION `имя_коллекции`;
```

## Параметры

* `имя_коллекции`: Имя удаляемой коллекции резервных копий.

{% note warning %}

Эта операция безвозвратно удаляет коллекцию резервных копий и все содержащиеся в ней резервные копии. Это действие не может быть отменено.

{% endnote %}

## Примеры

Удаление коллекции резервных копий:

```yql
DROP BACKUP COLLECTION `old_backups`;
```

## См. также

* [Концепции коллекций резервных копий](../../../concepts/backup-collections.md)
* [CREATE BACKUP COLLECTION](create-backup-collection.md)
* [BACKUP](backup.md)
* [RESTORE](restore-backup-collection.md)
