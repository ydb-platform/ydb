# Стратегия резервного копирования для микросервисов

Организация коллекций резервных копий по границам сервисов для независимых операций резервного копирования и восстановления.

## Коллекции для отдельных сервисов

Создайте отдельные коллекции резервных копий для каждого микросервиса:

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

## Независимое резервное копирование

Выполняйте резервное копирование каждого сервиса по собственному расписанию:

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

## Преимущества коллекций для отдельных сервисов

- **Независимое восстановление**: восстановление отдельных сервисов без влияния на другие
- **Гибкое расписание**: разная частота резервного копирования для сервисов в зависимости от критичности
- **Уменьшение зоны поражения**: проблемы с одной коллекцией не влияют на другие
- **Владение командами**: каждая команда может управлять своей стратегией резервного копирования

## Следующие шаги

- [Экспорт резервных копий во внешнее хранилище](exporting-to-external-storage.md)
- [Импорт и восстановление резервных копий](importing-and-restoring.md)
