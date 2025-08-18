# Создание коллекций резервных копий

Данное руководство описывает пошаговый процесс создания коллекций резервных копий и организации первичного резервного копирования.

## Подготовка

### Определение таблиц для резервного копирования

Перед созданием коллекции определите:

1. **Какие таблицы включить**: Логически связанные таблицы (например, заказы и продукты)
2. **Стратегию именования**: Используйте осмысленные имена коллекций
3. **Частоту резервного копирования**: Полные еженедельно, инкрементальные ежедневно

### Проверка существования таблиц

Убедитесь, что все таблицы существуют:

```sql
-- Проверка существования таблиц
SELECT * FROM `.sys/tables` WHERE path LIKE '/Root/database/%';
```

## Шаг 1: Создание коллекции

### Базовый пример

```sql
CREATE BACKUP COLLECTION `orders_backup`
    ( TABLE `/Root/shop/orders`
    )
WITH
    ( STORAGE = 'cluster'
    );
```

### Коллекция для нескольких таблиц

```sql
CREATE BACKUP COLLECTION `shop_full_backup`
    ( TABLE `/Root/shop/orders`
    , TABLE `/Root/shop/products`
    , TABLE `/Root/shop/customers`
    , TABLE `/Root/shop/categories`
    )
WITH
    ( STORAGE = 'cluster'
    );
```

### Использование переменных для гибкости

```sql
-- Определение переменных
$timestamp = CAST(CurrentUtcTimestamp() AS Utf8);
$collection_name = "backup_" || SUBSTRING($timestamp, 0, 10); -- YYYY-MM-DD
$orders_table = "/Root/production/orders";
$products_table = "/Root/production/products";

-- Создание коллекции с переменными
CREATE BACKUP COLLECTION $collection_name
    ( TABLE $orders_table
    , TABLE $products_table
    )
WITH
    ( STORAGE = 'cluster'
    );
```

## Шаг 2: Проверка создания коллекции

После создания коллекции проверьте её статус:

```bash
# Проверка через CLI (если поддерживается)
{{ ydb-cli }} backup collection list
```

## Шаг 3: Создание первого полного бэкапа

Первая резервная копия коллекции всегда должна быть полной:

```sql
-- Создание базового полного бэкапа
BACKUP `shop_full_backup`;
```

### Отслеживание операции

```bash
# Проверка статуса операции
{{ ydb-cli }} operation list incbackup

# Получение деталей последней операции
OPERATION_ID=$({{ ydb-cli }} operation list incbackup --format proto-json-base64 | jq -r '.operations[0].id')
{{ ydb-cli }} operation get "$OPERATION_ID"
```

## Шаг 4: Настройка регулярных инкрементальных бэкапов

После создания полного бэкапа можно настроить инкрементальные:

```sql
-- Пример ежедневного инкрементального бэкапа
BACKUP `shop_full_backup` INCREMENTAL;
```

## Примеры конфигураций

### Конфигурация 1: Небольшая база данных

```sql
-- Для небольших приложений
CREATE BACKUP COLLECTION `app_backup`
    ( TABLE `/Root/app/users`
    , TABLE `/Root/app/sessions`
    , TABLE `/Root/app/settings`
    )
WITH ( STORAGE = 'cluster' );

-- Еженедельный полный бэкап
BACKUP `app_backup`;
```

### Конфигурация 2: Разделение по функциональности

```sql
-- Коллекция для пользовательских данных
CREATE BACKUP COLLECTION `user_data_backup`
    ( TABLE `/Root/app/users`
    , TABLE `/Root/app/profiles`
    , TABLE `/Root/app/preferences`
    )
WITH ( STORAGE = 'cluster' );

-- Коллекция для транзакционных данных
CREATE BACKUP COLLECTION `transactions_backup`
    ( TABLE `/Root/app/orders`
    , TABLE `/Root/app/payments`
    , TABLE `/Root/app/invoices`
    )
WITH ( STORAGE = 'cluster' );

-- Создание полных бэкапов для обеих коллекций
BACKUP `user_data_backup`;
BACKUP `transactions_backup`;
```

### Конфигурация 3: Временные коллекции для тестирования

```sql
-- Коллекция для тестовых данных
CREATE BACKUP COLLECTION `test_backup_2024_08_18`
    ( TABLE `/Root/test/sample_data`
    )
WITH ( STORAGE = 'cluster' );

BACKUP `test_backup_2024_08_18`;
```

## Автоматизация создания коллекций

### Скрипт для создания коллекции

```bash
#!/bin/bash
# create_backup_collection.sh

COLLECTION_NAME="$1"
DATABASE_PATH="$2"

if [ -z "$COLLECTION_NAME" ] || [ -z "$DATABASE_PATH" ]; then
    echo "Использование: $0 <collection_name> <database_path>"
    echo "Пример: $0 prod_backup /Root/production"
    exit 1
fi

# Создание SQL для коллекции
cat << EOF > create_collection.sql
CREATE BACKUP COLLECTION \`${COLLECTION_NAME}\`
    ( TABLE \`${DATABASE_PATH}/orders\`
    , TABLE \`${DATABASE_PATH}/products\`
    , TABLE \`${DATABASE_PATH}/customers\`
    )
WITH ( STORAGE = 'cluster' );

BACKUP \`${COLLECTION_NAME}\`;
EOF

# Выполнение SQL
{{ ydb-cli }} yql -f create_collection.sql

echo "Коллекция '$COLLECTION_NAME' создана и первый полный бэкап запущен"

# Отслеживание операции
sleep 3
OPERATION_ID=$({{ ydb-cli }} operation list incbackup --format proto-json-base64 | jq -r '.operations[0].id')
if [ ! -z "$OPERATION_ID" ]; then
    echo "ID операции: $OPERATION_ID"
    {{ ydb-cli }} operation get "$OPERATION_ID"
fi

# Cleanup
rm create_collection.sql
```

### Использование скрипта

```bash
# Создание продакшн коллекции
./create_backup_collection.sh "prod_backup_$(date +%Y_%m)" "/Root/production"

# Создание тестовой коллекции
./create_backup_collection.sh "test_backup" "/Root/testing"
```

## Проверка и валидация

### Проверка успешного создания

```bash
# Проверка списка операций
{{ ydb-cli }} operation list incbackup --format pretty

# Проверка что операция завершилась успешно
OPERATION_ID="<operation-id>"
{{ ydb-cli }} operation get "$OPERATION_ID" --format pretty | grep -E "(status|ready)"
```

### Проверка экспорта

```bash
# Экспорт созданной коллекции для проверки
ydb tools dump -p .backups/collections/shop_full_backup -o test_export

# Проверка структуры экспорта
ls -la test_export/
find test_export/ -name "*.json" -exec echo "=== {} ===" \; -exec cat {} \;
```

## Лучшие практики

### Именование коллекций

1. **Включайте дату**: `backup_2024_08_18`
2. **Указывайте назначение**: `user_data_backup`, `transaction_backup`
3. **Добавляйте окружение**: `prod_backup`, `staging_backup`

```sql
-- Хорошие примеры имен
CREATE BACKUP COLLECTION `prod_orders_backup_2024_08`;
CREATE BACKUP COLLECTION `staging_full_backup`;
CREATE BACKUP COLLECTION `migration_test_backup`;
```

### Группировка таблиц

1. **По функциональности**: Группируйте логически связанные таблицы
2. **По частоте изменений**: Часто изменяемые таблицы в отдельную коллекцию
3. **По критичности**: Критически важные данные отдельно

### Планирование

1. **Полные бэкапы**: Еженедельно в период низкой нагрузки
2. **Инкрементальные бэкапы**: Ежедневно
3. **Мониторинг**: Регулярная проверка статуса операций

## Устранение проблем

### Ошибка: коллекция уже существует

```text
Error: Backup collection 'my_backup' already exists
```

**Решение**: Используйте другое имя или удалите существующую коллекцию (если возможно)

### Ошибка: таблица не найдена

```text
Error: Table '/Root/database/nonexistent' not found
```

**Решение**: Проверьте правильность пути к таблице и её существование

### Ошибка: недостаточно прав

```text
Error: Access denied for backup operation
```

**Решение**: Убедитесь, что у пользователя есть права на создание бэкапов

## См. также

- [SQL API](sql-api.md) - Полный справочник команд
- [Инкрементальные бэкапы](incremental-backups.md) - Работа с инкрементальными копиями
- [Управление операциями](manage-collections.md) - Мониторинг и управление
- [Восстановление](restore-from-collection.md) - Процедуры восстановления
