# Восстановление из коллекций резервных копий

Данное руководство описывает процедуры восстановления данных из коллекций резервных копий.

## Основы восстановления

### Принцип работы

Восстановление из коллекции резервных копий происходит через экспорт и импорт:

1. **Экспорт**: Извлечение данных из кластерного хранилища в файловую систему
2. **Импорт**: Загрузка данных из файловой системы в целевую базу данных

### Типы восстановления

- **Полное восстановление**: Восстановление всех таблиц коллекции
- **Восстановление на момент времени**: Восстановление на определенный бэкап в цепочке
- **Частичное восстановление**: Восстановление отдельных таблиц

## Экспорт коллекции

### Базовый экспорт

```bash
# Экспорт всей коллекции
ydb tools dump -p .backups/collections/collection_name -o exported_backup

# Просмотр структуры экспортированных данных
ls -la exported_backup/

### Экспорт конкретного бэкапа

```bash
# Экспорт полного бэкапа (базового)
ydb tools dump -p .backups/collections/shop_backup/backup_20240818_full -o full_backup_export

# Экспорт инкрементального бэкапа
ydb tools dump -p .backups/collections/shop_backup/backup_20240819_incremental -o incremental_backup_export
```

### Анализ экспортированных данных

```bash
# Просмотр метаданных экспорта
find exported_backup/ -name "*.json" -exec echo "=== {} ===" \; -exec cat {} \;

# Подсчет размеров бэкапов
du -sh exported_backup/*/

## Импорт и восстановление

### Восстановление полного бэкапа

```bash
# Восстановление базового полного бэкапа
ydb tools restore -p ".backups/collections/target_collection/restored_full" -i "full_backup_export/backup_dir"
```

### Поэтапное восстановление цепочки

```bash
#!/bin/bash
# restore_chain.sh - Восстановление полной цепочки бэкапов

COLLECTION_NAME="target_collection"
EXPORT_DIR="exported_backup"

echo "Начинаем восстановление цепочки бэкапов..."

# Восстанавливаем в правильном порядке: сначала полный, затем инкрементальные
for backup_dir in $EXPORT_DIR/*/; do
    backup_name=$(basename "$backup_dir")

    case "$backup_name" in
        *_full*)
            echo "Восстанавливаем полный бэкап: $backup_name"
            ydb tools restore -p ".backups/collections/$COLLECTION_NAME/$backup_name" -i "$backup_dir"
            ;;
    esac
done

# Затем восстанавливаем инкрементальные бэкапы в хронологическом порядке
for backup_dir in $EXPORT_DIR/*/; do
    backup_name=$(basename "$backup_dir")

    case "$backup_name" in
        *_incremental*)
            echo "Восстанавливаем инкрементальный бэкап: $backup_name"
            ydb tools restore -p ".backups/collections/$COLLECTION_NAME/$backup_name" -i "$backup_dir"
            ;;
    esac
done

echo "Восстановление цепочки завершено"
```

## Мониторинг операций восстановления

### Отслеживание прогресса

```bash
# Список всех операций восстановления
{{ ydb-cli }} operation list restore
```

```bash
# Получение статуса конкретной операции восстановления
{{ ydb-cli }} operation get "ydb://restore/N?id=<operation-id>"
```

```bash
# Автоматическое отслеживание последней операции
RESTORE_OPERATION_ID=$({{ ydb-cli }} operation list restore --format proto-json-base64 | jq -r '.operations[0].id')
{{ ydb-cli }} operation get "$RESTORE_OPERATION_ID"
```

### Ожидание завершения операции

```bash
# Скрипт ожидания завершения восстановления
wait_for_restore_completion() {
    local operation_id="$1"

    while true; do
        status=$({{ ydb-cli }} operation get "$operation_id" --format proto-json-base64 | jq -r '.ready')

        if [ "$status" = "true" ]; then
            echo "Операция восстановления завершена: $operation_id"
            {{ ydb-cli }} operation get "$operation_id"
            break
        fi

        echo "Операция восстановления в процессе..."
        sleep 30
    done
}
```

## См. также

- [SQL API](sql-api.md) - Справочник команд резервного копирования
- [Создание коллекций](create-collection.md) - Настройка и создание коллекций
- [Инкрементальные бэкапы](incremental-backups.md) - Работа с инкрементальными копиями
- [Управление операциями](manage-collections.md) - Мониторинг и управление операциями
