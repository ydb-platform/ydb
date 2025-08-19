# SQL API: Коллекции резервных копий

Этот раздел предоставляет полный справочник SQL команд, используемых с коллекциями резервных копий.

## CREATE BACKUP COLLECTION

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
```

#### Создание коллекции с использованием переменных

```sql
$collection_name = "my_backup_" || CAST(CurrentUtcTimestamp() AS Utf8);
$orders_table = "/Root/database/orders";

CREATE BACKUP COLLECTION $collection_name
    ( TABLE $orders_table
## BACKUP

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

## DROP BACKUP COLLECTION

Удаляет коллекцию резервных копий и все связанные с ней резервные копии.

### Синтаксис

```sql
DROP BACKUP COLLECTION <collection_name>;
```

{% note warning %}

Эта операция необратима и удалит все резервные копии в коллекции. Убедитесь, что у вас есть альтернативные резервные копии перед удалением коллекции.

{% endnote %}

## Запрос информации о резервных копиях

Просмотр коллекций через схему базы данных:

```bash
# Список всех коллекций
ydb scheme ls .backups/collections/

# Просмотр структуры конкретной коллекции
ydb scheme ls .backups/collections/shop_backups/
```

## Ограничения и соображения

### Текущие ограничения

- **Вариант хранения**: В настоящее время через SQL поддерживается только хранение 'cluster'
- **Изменение коллекции**: Нельзя добавлять или удалять таблицы из существующих коллекций
- **Параллельные резервные копии**: Множественные операции резервного копирования одной коллекции могут конфликтовать

Подробные соображения производительности и рекомендации по управлению цепочками см. в [Концепциях](concepts.md).
    )
WITH ( STORAGE = 'cluster' );

BACKUP `weekly_backup`;
```

```sql
-- Вторник-воскресенье: инкрементальные бэкапы
BACKUP `weekly_backup` INCREMENTAL;
```

### Сценарий 2: Бэкап перед важными изменениями

```sql
-- Создание точки восстановления перед миграцией
CREATE BACKUP COLLECTION `pre_migration_backup`
    ( TABLE `/Root/production/critical_table`
    )
WITH ( STORAGE = 'cluster' );

## Следующие шаги

- [Изучите концепции коллекций резервных копий](concepts.md)
- [Исследуйте все операции и задачи управления](operations.md)
