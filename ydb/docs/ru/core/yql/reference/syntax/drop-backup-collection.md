# DROP BACKUP COLLECTION

{% include [feature_enterprise.md](../../../_includes/feature_enterprise.md) %}

Выражение `DROP BACKUP COLLECTION` удаляет [коллекцию резервных копий](../../../concepts/datamodel/backup-collection.md) и все содержащиеся в ней резервные копии.

```yql
DROP BACKUP COLLECTION collection_name;
```

## Параметры

* `collection_name` — имя удаляемой коллекции резервных копий.

{% note warning %}

Эта операция безвозвратно удаляет коллекцию резервных копий и все содержащиеся в ней резервные копии из кластера. Это действие не может быть отменено.

{% endnote %}

{% note info %}

Удаление коллекции резервных копий затрагивает только данные в кластере. Резервные копии, ранее экспортированные во внешнее хранилище (S3 или файловую систему), не затрагиваются и остаются доступными для импорта.

{% endnote %}

## Примеры

Удаление коллекции резервных копий:

```yql
DROP BACKUP COLLECTION old_backups;
```

## См. также

* [Коллекции резервных копий](../../../concepts/datamodel/backup-collection.md).
* [CREATE BACKUP COLLECTION](create-backup-collection.md).
* [BACKUP](backup.md).
* [RESTORE](restore-backup-collection.md).
