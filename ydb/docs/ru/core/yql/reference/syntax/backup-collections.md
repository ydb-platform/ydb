# Команды коллекций резервных копий

YQL поддерживает SQL команды для управления [коллекциями резервных копий](../../../concepts/backup/collections.md). Эти команды позволяют создавать, управлять и создавать резервные копии таблиц базы данных с использованием декларативного SQL синтаксиса.

Для практических примеров использования и руководства по операциям см. [руководство по операциям с коллекциями резервных копий](../../../reference/ydb-cli/export-import/backup-collections/operations.md).

## CREATE BACKUP COLLECTION {#create-backup-collection}

Создает новую коллекцию резервных копий с указанными таблицами и конфигурацией.

### Синтаксис

```sql
CREATE BACKUP COLLECTION <collection_name>
    ( TABLE <table_path> [, TABLE <table_path>]... )
WITH ( STORAGE = '<storage_backend>'
     [, INCREMENTAL_BACKUP_ENABLED = '<true|false>']
     [, <additional_options>] );
```

### Параметры

- **collection_name**: Уникальный идентификатор коллекции (должен быть заключен в обратные кавычки)
- **table_path**: Абсолютный путь к таблице для включения в коллекцию
- **STORAGE**: Тип варианта хранения (в настоящее время поддерживает 'cluster')
- **INCREMENTAL_BACKUP_ENABLED**: Включить или отключить поддержку инкрементальных резервных копий

### Примеры

**Создание базовой коллекции:**

```sql
CREATE BACKUP COLLECTION `my_backups`
    ( TABLE `/Root/database/users` )
WITH ( STORAGE = 'cluster' );
```

**Коллекция с инкрементальными резервными копиями (рекомендуется):**

```sql
CREATE BACKUP COLLECTION `shop_backups`
    ( TABLE `/Root/shop/orders`, TABLE `/Root/shop/products` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

## BACKUP {#backup}

Создает резервную копию в существующей коллекции. Первая резервная копия всегда является полной; последующие резервные копии могут быть инкрементальными.

### Синтаксис

```sql
BACKUP <collection_name> [INCREMENTAL];
```

### Параметры

- **collection_name**: Имя существующей коллекции резервных копий
- **INCREMENTAL**: Необязательное ключевое слово для создания инкрементальной резервной копии

### Типы резервных копий

**Полная резервная копия (по умолчанию для первой резервной копии):**

```sql
BACKUP `shop_backups`;
```

**Инкрементальная резервная копия:**

```sql
BACKUP `shop_backups` INCREMENTAL;
```

### Пример полного рабочего процесса

```sql
-- 1. Создать коллекцию
CREATE BACKUP COLLECTION `sales_data`
    ( TABLE `/Root/sales/transactions`, TABLE `/Root/sales/customers` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- 2. Создать начальную полную резервную копию
BACKUP `sales_data`;

-- 3. Создать инкрементальные резервные копии
BACKUP `sales_data` INCREMENTAL;
```

## DROP BACKUP COLLECTION {#drop-backup-collection}

Удаляет коллекцию резервных копий и все связанные с ней резервные копии.

### Синтаксис

```sql
DROP BACKUP COLLECTION <collection_name>;
```

{% note warning %}

Эта операция необратима и удалит все резервные копии в коллекции. Убедитесь, что у вас есть альтернативные резервные копии перед удалением коллекции.

{% endnote %}

## Ограничения и соображения {#limitations}

### Текущие ограничения

- **Варианты хранения**: В настоящее время поддерживается только хранилище 'cluster' через SQL
- **Модификация коллекции**: Невозможно добавлять или удалять таблицы из существующих коллекций  
- **Параллельные резервные копии**: Несколько операций резервного копирования в одной коллекции могут конфликтовать

## См. также

- [Концепции коллекций резервных копий](../../concepts/backup/collections.md)
- [Руководство по операциям коллекций резервных копий](../../reference/ydb-cli/export-import/backup-collections/operations.md)
