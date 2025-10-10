# Коллекции резервных копий: Распространенные рецепты и примеры {#backup-collections-recipes}

Данное руководство предоставляет практические примеры для распространенных случаев использования коллекций резервных копий. Для базовых операций см. [руководство по операциям](../maintenance/manual/backup-collections.md).

## Базовый рабочий процесс резервного копирования {#basic-workflow}

### Создание первой коллекции резервных копий {#creating-first-collection}

```sql
-- Создание коллекции для связанных таблиц
CREATE BACKUP COLLECTION `production_backups`
    ( TABLE `/Root/shop/orders`
    , TABLE `/Root/shop/products`
    , TABLE `/Root/shop/customers`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

### Создание резервных копий {#creating-backups}

```sql
-- Создание первоначальной полной резервной копии
BACKUP `production_backups`;

-- Позже создание инкрементальных резервных копий
BACKUP `production_backups` INCREMENTAL;
```

### Мониторинг операций резервного копирования {#monitoring-backup-operations}

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
CREATE BACKUP COLLECTION `dev_test_backups`
    ( TABLE `/Root/dev/users`
    , TABLE `/Root/dev/test_data`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Ежедневные полные резервные копии в среде разработки
BACKUP `dev_test_backups`;
```

### Производственная среда {#production-environment}

```sql
-- Создание комплексной коллекции для продакшена
CREATE BACKUP COLLECTION `prod_daily_backups`
    ( TABLE `/Root/production/orders`
    , TABLE `/Root/production/products`
    , TABLE `/Root/production/customers`
    , TABLE `/Root/production/inventory`
    , TABLE `/Root/production/transactions`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Еженедельная полная резервная копия
BACKUP `prod_daily_backups`;

-- Ежедневные инкрементальные резервные копии
BACKUP `prod_daily_backups` INCREMENTAL;
```

## Стратегия резервного копирования микросервисов {#microservices}

### Коллекции для отдельных сервисов {#individual-service-collections}

```sql
-- Коллекция резервных копий пользовательского сервиса
CREATE BACKUP COLLECTION `user_service_backups`
    ( TABLE `/Root/users/profiles`
    , TABLE `/Root/users/preferences`
    , TABLE `/Root/users/sessions`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Коллекция резервных копий сервиса заказов
CREATE BACKUP COLLECTION `order_service_backups`
    ( TABLE `/Root/orders/orders`
    , TABLE `/Root/orders/order_items`
    , TABLE `/Root/orders/payments`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Коллекция резервных копий сервиса инвентаря
CREATE BACKUP COLLECTION `inventory_service_backups`
    ( TABLE `/Root/inventory/products`
    , TABLE `/Root/inventory/stock_levels`
    , TABLE `/Root/inventory/warehouses`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

### Рабочий процесс резервного копирования сервисов {#service-backup-workflow}

```sql
-- Создание резервных копий для каждого сервиса независимо
BACKUP `user_service_backups`;
BACKUP `order_service_backups`;
BACKUP `inventory_service_backups`;

-- Позже создание инкрементальных резервных копий
BACKUP `user_service_backups` INCREMENTAL;
BACKUP `order_service_backups` INCREMENTAL;
BACKUP `inventory_service_backups` INCREMENTAL;
```

## Экспорт данных и восстановление {#export-recovery}

### Экспорт коллекций резервных копий {#exporting-backup-collections}

Для аварийного восстановления или миграции экспортируйте коллекции резервных копий с использованием стандартных инструментов YDB:

```bash
# Экспорт коллекции резервных копий
ydb tools dump -p .backups/collections/production_backups -o /backup/exports/production_backups_export

# Экспорт конкретной резервной копии из коллекции
ydb tools dump -p .backups/collections/production_backups/backup_20240315_120000 -o /backup/exports/backup_20240315
```

### Импорт в целевую базу данных {#importing-to-target-database}

```bash
# Импорт резервной копии в целевую базу данных
ydb tools restore -i /backup/exports/production_backups_export -d /Root/restored_production

# Импорт конкретной резервной копии
ydb tools restore -i /backup/exports/backup_20240315 -d /Root/restored_data
```

## Управление коллекциями {#collection-management}

### Проверка статуса коллекции {#checking-collection-status}

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
DROP BACKUP COLLECTION `old_collection_name`;
```

## Простая проверка резервных копий {#validation}

### Проверка завершения резервного копирования {#checking-backup-completion}

```bash
# Проверка успешного завершения операции резервного копирования
ydb operation list incbackup | grep -E "(COMPLETED|FAILED)"

# Проверка существования каталога резервной копии
ydb scheme ls .backups/collections/production_backups/ | tail -1
```

### Базовое тестирование резервных копий {#basic-backup-testing}

```bash
# Экспорт недавней резервной копии для тестирования
ydb tools dump -p .backups/collections/production_backups/backup_20240315_120000 -o /tmp/test_restore

# Тестовое восстановление во временное место (в тестовой среде)
ydb tools restore -i /tmp/test_restore -d /Root/test_restore_verification
```

## Сводка лучших практик {#best-practices}

### Стратегия резервного копирования {#backup-strategy}

- **Управление длиной цепочки**: Периодически создавайте новые полные резервные копии, чтобы избежать чрезмерно длинных инкрементальных цепочек.
- **Отдельные коллекции по сервисам**: Используйте разные коллекции для разных приложений/сервисов.
- **Регулярные полные резервные копии**: Создавайте полные резервные копии еженедельно или раз в две недели.
- **Регулярное тестирование**: Периодически проверяйте, что резервные копии можно восстановить.

### Операции {#operations}

- **Мониторинг статуса операций**: Всегда проверяйте завершение операций резервного копирования.
- **Ручная очистка**: Удаляйте старые цепи резервных копий вручную для управления хранилищем.
- **Документирование процедур**: Ведите документацию процедур резервного копирования и восстановления.
- **Планирование для катастроф**: Практикуйте процедуры восстановления в непроизводственных средах.

### Управление хранилищем {#storage-management}

- **Мониторинг использования хранилища**: Отслеживайте потребление хранилища резервных копий.
- **Планирование сохранности**: Определите, как долго хранить цепи резервных копий.
- **Экспорт важных резервных копий**: Используйте экспорт/импорт для долгосрочного архивирования.
- **Проверка целостности цепи**: Никогда не удаляйте частичные цепи резервных копий.

## Распространенные проблемы и решения {#troubleshooting}

### Сбои операций резервного копирования {#backup-operation-failures}

- **Проверка разрешений**: Убедитесь, что пользователь имеет разрешения на операции резервного копирования.
- **Проверка доступа к таблицам**: Подтвердите, что все таблицы в коллекции доступны.
- **Мониторинг ресурсов**: Проверьте системные ресурсы во время операций резервного копирования.

### Проблемы с хранилищем {#storage-issues}

- **Очистка старых резервных копий**: Удалите старые цепи резервных копий для освобождения места.
- **Мониторинг размеров резервных копий**: Отслеживайте рост инкрементальных резервных копий.
- **Планирование емкости хранилища**: Оцените потребности в хранилище для сохранения резервных копий.

### Управление цепями {#chain-management}

- **Избегайте частичных удалений**: Никогда не удаляйте отдельные резервные копии из цепи.
- **Документируйте разрывы цепи**: Ведите учет любых прерываний цепи резервных копий.
- **Создавайте новые цепи**: Создавайте новые полные резервные копии, когда цепи становятся слишком длинными.

## См. также {#see-also}

- [Концепции коллекций резервных копий](../concepts/backup-collections.md) - Основные концепции и архитектура.
- [Руководство по операциям](../maintenance/manual/backup-collections.md) - Подробные операционные процедуры.
