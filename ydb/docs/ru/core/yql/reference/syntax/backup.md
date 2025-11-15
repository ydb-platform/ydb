# BACKUP

Выражение `BACKUP` создает резервную копию таблиц в [коллекции резервных копий](../../../concepts/backup-collections.md).

```yql
BACKUP collection_name [INCREMENTAL];
```

## Параметры

* `collection_name` — имя коллекции резервных копий.
* `INCREMENTAL` — создание инкрементальной резервной копии вместо полной.

## Типы резервных копий

### Полная резервная копия

Полная резервная копия создает снимок всех таблиц в коллекции на определенный момент времени. Это служит основой для последующих инкрементальных резервных копий.

```yql
BACKUP production_backups;
```

### Инкрементальная резервная копия

Инкрементальная резервная копия захватывает только изменения (вставки, обновления, удаления) с момента предыдущей резервной копии в цепочке. Коллекция должна быть создана с `INCREMENTAL_BACKUP_ENABLED = 'true'`.

```yql
BACKUP production_backups INCREMENTAL;
```

{% note warning %}

Инкрементальные резервные копии требуют предыдущей полной резервной копии в той же коллекции. Всегда сначала создавайте полную резервную копию, прежде чем делать инкрементальные резервные копии.

{% endnote %}

## Примеры

Создание начальной полной резервной копии:

```yql
-- Сначала создайте коллекцию
CREATE BACKUP COLLECTION daily_backups (
    TABLE orders
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);

-- Затем создайте полную резервную копию
BACKUP daily_backups;
```

Создание инкрементальных резервных копий:

```yql
-- После начальной полной резервной копии создайте инкрементальные резервные копии
BACKUP daily_backups INCREMENTAL;
```

## Мониторинг операций резервного копирования

Операции резервного копирования выполняются асинхронно в фоновом режиме. Вы можете отслеживать их прогресс с помощью {{ ydb-short-name }} CLI:

```bash
# Список операций резервного копирования
ydb operation list incbackup

# Получить детали операции
ydb operation get <operation-id>
```

## См. также

* [Коллекции резервных копий](../../../concepts/backup-collections.md).
* [CREATE BACKUP COLLECTION](create-backup-collection.md).
* [RESTORE](restore-backup-collection.md).
* [DROP BACKUP COLLECTION](drop-backup-collection.md).
* [Руководство по операциям с коллекциями резервных копий](../../../maintenance/manual/backup-collections.md).
