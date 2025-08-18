# SQL API для коллекций резервных копий

Данная страница содержит полный справочник SQL команд для работы с коллекциями резервных копий.

## CREATE BACKUP COLLECTION

Создает новую коллекцию резервных копий для указанного набора таблиц.

### Синтаксис

```sql
CREATE BACKUP COLLECTION collection_name
    ( TABLE table_path [, TABLE table_path ...]
    )
WITH
    ( STORAGE = 'cluster'
    [, option_name = 'option_value' ...]
    );
```

### Параметры

- `collection_name` - Имя коллекции резервных копий (строка в обратных кавычках)
- `table_path` - Полный путь к таблице в формате `/Root/database/table_name`
- `STORAGE` - Тип хранилища. В настоящее время поддерживается только `'cluster'`

### Примеры

#### Создание коллекции для одной таблицы

```sql
CREATE BACKUP COLLECTION `orders_backup`
    ( TABLE `/Root/shop/orders`
    )
WITH
    ( STORAGE = 'cluster'
    );
```

#### Создание коллекции для нескольких таблиц

```sql
CREATE BACKUP COLLECTION `shop_backup`
    ( TABLE `/Root/shop/orders`
    , TABLE `/Root/shop/products`
    , TABLE `/Root/shop/customers`
    )
WITH
    ( STORAGE = 'cluster'
    );
```

#### Создание коллекции с использованием переменных

```sql
$collection_name = "my_backup_" || CAST(CurrentUtcTimestamp() AS Utf8);
$orders_table = "/Root/database/orders";

CREATE BACKUP COLLECTION $collection_name
    ( TABLE $orders_table
    )
WITH
    ( STORAGE = 'cluster'
    );
```

### Особенности

- Имена коллекций должны быть уникальными в рамках базы данных
- Таблицы должны существовать на момент создания коллекции
- Один раз созданную коллекцию нельзя изменить (добавить/удалить таблицы)
- Путь к таблице должен быть абсолютным

## BACKUP

Создает резервную копию коллекции - полную или инкрементальную.

### Синтаксис

```sql
-- Полная резервная копия
BACKUP collection_name;

-- Инкрементальная резервная копия
BACKUP collection_name INCREMENTAL;
```

### Параметры

- `collection_name` - Имя существующей коллекции резервных копий
- `INCREMENTAL` - Ключевое слово для создания инкрементального бэкапа

### Примеры

#### Создание полного бэкапа

```sql
-- Создать полную резервную копию коллекции
BACKUP `shop_backup`;
```

#### Создание инкрементального бэкапа

```sql
-- Создать инкрементальную резервную копию
BACKUP `shop_backup` INCREMENTAL;
```

#### Создание бэкапов в цикле

```sql
-- Пример создания серии инкрементальных бэкапов
BACKUP `daily_backup` INCREMENTAL; -- День 1
-- ... изменения данных ...
BACKUP `daily_backup` INCREMENTAL; -- День 2
-- ... изменения данных ...
BACKUP `daily_backup` INCREMENTAL; -- День 3
```

### Особенности

- Первый бэкап коллекции всегда должен быть полным
- Инкрементальный бэкап можно создать только если уже существует базовый полный бэкап
- Операции выполняются асинхронно в фоновом режиме
- Можно отслеживать прогресс через `operation list incbackup`

## Управление операциями

### Отслеживание операций

```bash
# Список всех активных операций резервного копирования
{{ ydb-cli }} operation list incbackup

# Статус конкретной операции
{{ ydb-cli }} operation get "ydb://incbackup/N?id=<operation-id>"

# Удаление завершенной операции
{{ ydb-cli }} operation forget "ydb://incbackup/N?id=<operation-id>"
```

### Форматы вывода

```bash
# Красивый формат для чтения
{{ ydb-cli }} operation list incbackup --format pretty

# JSON для автоматизации
{{ ydb-cli }} operation list incbackup --format proto-json-base64
```

## Практические примеры

### Сценарий 1: Еженедельный цикл бэкапов

```sql
-- Понедельник: создание коллекции и полного бэкапа
CREATE BACKUP COLLECTION `weekly_backup`
    ( TABLE `/Root/app/transactions`
    , TABLE `/Root/app/user_data`
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

BACKUP `pre_migration_backup`;

-- Выполнение миграции...
-- При необходимости можно создать инкрементальный бэкап после
BACKUP `pre_migration_backup` INCREMENTAL;
```

### Сценарий 3: Автоматизация через скрипты

```bash
#!/bin/bash
# Скрипт для ежедневного инкрементального бэкапа

COLLECTION_NAME="daily_prod_backup"

# Создание инкрементального бэкапа
{{ ydb-cli }} yql -s "BACKUP \`${COLLECTION_NAME}\` INCREMENTAL;"

# Отслеживание операции
sleep 5
OPERATION_ID=$({{ ydb-cli }} operation list incbackup --format proto-json-base64 | jq -r '.operations[0].id')

if [ ! -z "$OPERATION_ID" ]; then
    echo "Отслеживаем операцию: $OPERATION_ID"
    {{ ydb-cli }} operation get "$OPERATION_ID"
fi
```

## Ограничения и рекомендации

### Ограничения

- Нельзя создать инкрементальный бэкап без базового полного
- Максимальная длина цепочки: 100 инкрементальных бэкапов
- Нельзя изменить состав таблиц в существующей коллекции
- Операции не могут выполняться параллельно для одной коллекции

### Рекомендации

- Создавайте полные бэкапы еженедельно или ежемесячно
- Используйте осмысленные имена коллекций с указанием даты или назначения
- Мониторьте длину цепочки инкрементальных бэкапов
- Очищайте завершенные операции для поддержания порядка
- Тестируйте процедуры восстановления регулярно

## См. также

- [Создание коллекций](create-collection.md) - Пошаговое руководство
- [Инкрементальные бэкапы](incremental-backups.md) - Детали работы с инкрементальными копиями
- [Управление операциями](manage-collections.md) - Мониторинг и управление операциями
- [Восстановление](restore-from-collection.md) - Процедуры восстановления данных
