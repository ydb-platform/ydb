# SQL API: Коллекции резервных копий

Этот раздел предоставляет руководство по использованию SQL команд с коллекциями резервных копий. Для полного справочника синтаксиса см. [Команды коллекций резервных копий](../../../yql/reference/syntax/backup-collections.md) в справочнике YQL.

## Краткий справочник

Основные SQL команды для коллекций резервных копий:

- `CREATE BACKUP COLLECTION` - Создает новую коллекцию резервных копий
- `BACKUP` - Создает резервную копию (полную или инкрементальную)  
- `DROP BACKUP COLLECTION` - Удаляет коллекцию и все резервные копии

Для подробного синтаксиса, параметров и примеров обратитесь к [справочнику синтаксиса YQL](../../../yql/reference/syntax/backup-collections.md).

## Базовый рабочий процесс

```sql
-- 1. Создать коллекцию
CREATE BACKUP COLLECTION `shop_backups`
    ( TABLE `/Root/shop/orders`, TABLE `/Root/shop/products` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- 2. Создать начальную полную резервную копию
BACKUP `shop_backups`;

-- 3. Создать инкрементальные резервные копии
BACKUP `shop_backups` INCREMENTAL;
```

## Запрос информации о резервных копиях

Просмотр коллекций через схему базы данных:

```bash
# Список всех коллекций
ydb scheme ls .backups/collections/

# Просмотр структуры конкретной коллекции  
ydb scheme ls .backups/collections/shop_backups/
```

## Следующие шаги

- [Полный справочник синтаксиса YQL для коллекций резервных копий](../../../yql/reference/syntax/backup-collections.md)
- [Изучите концепции коллекций резервных копий](concepts.md)
- [Изучите все операции и задачи управления](operations.md)
