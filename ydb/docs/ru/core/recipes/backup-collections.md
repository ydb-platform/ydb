# Рецепты и примеры

Примеры типовых сценариев работы с коллекциями резервных копий.

## Базовый процесс {#basic-workflow}

### Создание первой коллекции резервных копий {#creating-first-collection}

```sql
-- Создание коллекции для связанных таблиц
CREATE BACKUP COLLECTION production_backups
    ( TABLE orders
    , TABLE products
    , TABLE customers
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

### Создание резервных копий {#creating-backups}

```sql
-- Создание первоначальной полной резервной копии
BACKUP production_backups;

-- Позже создание инкрементальных резервных копий
BACKUP production_backups INCREMENTAL;
```

### Мониторинг {#monitoring-backup-operations}

```bash
# Проверка статуса операций резервного копирования
ydb operation list incbackup

# Получение подробностей для конкретной операции
ydb operation get <operation-id>

# Просмотр коллекций резервных копий
ydb scheme ls .backups/collections/

# Список резервных копий в коллекции
ydb scheme ls .backups/collections/production_backups/
```

## Настройка для нескольких сред {#multi-environment}

### Среда разработки {#development-environment}

```sql
-- Создание коллекции с меньшим количеством таблиц для тестирования
CREATE BACKUP COLLECTION dev_test_backups
    ( TABLE users
    , TABLE test_data
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Ежедневные полные резервные копии в среде разработки
BACKUP dev_test_backups;
```

### Производственная среда {#production-environment}

```sql
-- Создание комплексной коллекции для продакшена
CREATE BACKUP COLLECTION prod_daily_backups
    ( TABLE orders
    , TABLE products
    , TABLE customers
    , TABLE inventory
    , TABLE transactions
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Еженедельная полная резервная копия
BACKUP prod_daily_backups;

-- Ежедневные инкрементальные резервные копии
BACKUP prod_daily_backups INCREMENTAL;
```

## Микросервисы {#microservices}

### Коллекции для отдельных сервисов {#individual-service-collections}

```sql
-- Коллекция резервных копий пользовательского сервиса
CREATE BACKUP COLLECTION user_service_backups
    ( TABLE profiles
    , TABLE preferences
    , TABLE sessions
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Коллекция резервных копий сервиса заказов
CREATE BACKUP COLLECTION order_service_backups
    ( TABLE orders
    , TABLE order_items
    , TABLE payments
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Коллекция резервных копий сервиса инвентаря
CREATE BACKUP COLLECTION inventory_service_backups
    ( TABLE products
    , TABLE stock_levels
    , TABLE warehouses
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

### Резервное копирование {#service-backup-workflow}

```sql
-- Создание резервных копий для каждого сервиса независимо
BACKUP user_service_backups;
BACKUP order_service_backups;
BACKUP inventory_service_backups;

-- Позже создание инкрементальных резервных копий
BACKUP user_service_backups INCREMENTAL;
BACKUP order_service_backups INCREMENTAL;
BACKUP inventory_service_backups INCREMENTAL;
```

## Экспорт данных и восстановление {#export-recovery}

### Экспорт {#exporting-backup-collections}

Экспорт коллекции с помощью YDB CLI:

```bash
# Экспорт коллекции резервных копий
ydb tools dump -p .backups/collections/production_backups -o /backup/exports/production_backups_export

# Экспорт конкретной резервной копии из коллекции
ydb tools dump -p .backups/collections/production_backups/backup_20240315_120000 -o /backup/exports/backup_20240315
```

### Импорт {#importing-to-target-database}

```bash
# Импорт резервной копии в целевую базу данных
ydb tools restore -i /backup/exports/production_backups_export -d /Root/restored_production

# Импорт конкретной резервной копии
ydb tools restore -i /backup/exports/backup_20240315 -d /Root/restored_data
```

## Управление коллекциями {#collection-management}

### Статус коллекции {#checking-collection-status}

```bash
# Список всех коллекций
ydb scheme ls .backups/collections/

# Проверка содержимого конкретной коллекции
ydb scheme ls .backups/collections/production_backups/ | sort

# Получение подробностей коллекции
ydb scheme describe .backups/collections/production_backups/
```

### Ручная очистка {#manual-cleanup}

```bash
# Удаление старых каталогов резервных копий (ручная очистка)
ydb scheme rmdir -r .backups/collections/production_backups/backup_20240301_120000/

# Всегда удаляйте полные цепи, никогда не удаляйте частичные цепи
# Пример: Удаление старой полной резервной копии и всех её инкрементальных копий
ydb scheme rmdir -r .backups/collections/production_backups/backup_20240301_120000/
ydb scheme rmdir -r .backups/collections/production_backups/backup_20240301_180000/
ydb scheme rmdir -r .backups/collections/production_backups/backup_20240302_060000/
```

### Жизненный цикл коллекции {#collection-lifecycle}

```sql
-- Удаление коллекции, когда она больше не нужна (удаляет коллекцию и все резервные копии)
DROP BACKUP COLLECTION old_collection_name;
```

## Проверка {#validation}

### Проверка завершения {#checking-backup-completion}

```bash
# Проверка успешного завершения операции резервного копирования
ydb operation list incbackup | grep -E "(COMPLETED|FAILED)"

# Проверка существования каталога резервной копии
ydb scheme ls .backups/collections/production_backups/ | tail -1
```

### Тестирование {#basic-backup-testing}

```bash
# Экспорт недавней резервной копии для тестирования
ydb tools dump -p .backups/collections/production_backups/backup_20240315_120000 -o /tmp/test_restore

# Тестовое восстановление во временное место (в тестовой среде)
ydb tools restore -i /tmp/test_restore -d /Root/test_restore_verification
```

## Лучшие практики {#best-practices}

### Стратегия {#backup-strategy}

- Периодически создавайте новые полные копии, чтобы цепочки не становились слишком длинными.
- Используйте разные коллекции для разных сервисов.
- Создавайте полные копии еженедельно или раз в две недели.
- Периодически проверяйте, что копии можно восстановить.

### Операции {#operations}

- Всегда проверяйте завершение операций.
- Удаляйте старые цепи вручную для экономии места.
- Документируйте процедуры резервного копирования и восстановления.
- Отрабатывайте восстановление в тестовых средах.

### Хранилище {#storage-management}

- Отслеживайте потребление дискового пространства.
- Определите политику хранения: как долго хранить цепи.
- Используйте экспорт для долгосрочного архивирования.
- Никогда не удаляйте частичные цепи.

## Проблемы и решения {#troubleshooting}

### Сбои операций {#backup-operation-failures}

- Проверьте права доступа пользователя.
- Убедитесь, что все таблицы в коллекции доступны.
- Проверьте системные ресурсы.

### Проблемы с хранилищем {#storage-issues}

- Удалите старые цепи для освобождения места.
- Отслеживайте рост инкрементальных копий.
- Оцените потребности в дисковом пространстве.

### Управление цепями {#chain-management}

- Никогда не удаляйте отдельные копии из середины цепи.
- Фиксируйте любые разрывы цепи.
- Начинайте новую цепь, когда старая становится слишком длинной.

## См. также {#see-also}

- [Концепции коллекций резервных копий](../concepts/backup-collections.md)
- [Руководство по операциям](../maintenance/manual/backup-collections.md)
