# Управление коллекциями и мониторинг операций

Данное руководство описывает процедуры мониторинга операций резервного копирования и восстановления, а также управление коллекциями.

## Мониторинг операций резервного копирования

### Список операций инкрементального резервного копирования

```bash
# Получить список всех операций инкрементального резервного копирования
{{ ydb-cli }} operation list incbackup
```

```bash
# Список с красивым форматированием для чтения
{{ ydb-cli }} operation list incbackup --format pretty
```

```bash
# Список в JSON формате для автоматизации
{{ ydb-cli }} operation list incbackup --format proto-json-base64
```

### Получение статуса конкретной операции

```bash
# Получение детальной информации об операции
{{ ydb-cli }} operation get "ydb://incbackup/N?id=<operation-id>"
```

```bash
# Получение статуса с красивым форматированием
{{ ydb-cli }} operation get "ydb://incbackup/N?id=<operation-id>" --format pretty
```

### Автоматическое получение ID последней операции

```bash
# Получение ID последней операции резервного копирования
OPERATION_ID=$({{ ydb-cli }} operation list incbackup --format proto-json-base64 | jq -r '.operations[0].id')

if [ ! -z "$OPERATION_ID" ]; then
    echo "Проверяем операцию: $OPERATION_ID"
    {{ ydb-cli }} operation get "$OPERATION_ID"
fi

## Мониторинг операций восстановления

### Список операций восстановления

```bash
# Получить список всех операций восстановления
{{ ydb-cli }} operation list restore
```

```bash
# Список с красивым форматированием
{{ ydb-cli }} operation list restore --format pretty
```

### Получение статуса операции восстановления

```bash
# Получение детальной информации об операции восстановления
{{ ydb-cli }} operation get "ydb://restore/N?id=<operation-id>"
```

## Управление операциями

### Удаление завершенных операций

```bash
# Удаление конкретной завершенной операции резервного копирования
{{ ydb-cli }} operation forget "ydb://incbackup/N?id=<operation-id>"
```

```bash
# Удаление конкретной завершенной операции восстановления
{{ ydb-cli }} operation forget "ydb://restore/N?id=<operation-id>"
```

### Автоматическая очистка завершенных операций

```bash
# Скрипт для очистки всех успешно завершенных операций резервного копирования
cleanup_backup_operations() {
    echo "Очищаем завершенные операции резервного копирования..."
    {{ ydb-cli }} operation list incbackup --format proto-json-base64 | \
    jq -r '.operations[] | select(.ready==true and .status=="SUCCESS") | .id' | \
    while read operation_id; do
        echo "Удаляем операцию: $operation_id"
        {{ ydb-cli }} operation forget "$operation_id"
    done
}

```bash
# Скрипт для очистки всех успешно завершенных операций восстановления
cleanup_restore_operations() {
    echo "Очищаем завершенные операции восстановления..."
    {{ ydb-cli }} operation list restore --format proto-json-base64 | \
    jq -r '.operations[] | select(.ready==true and .status=="SUCCESS") | .id' | \
    while read operation_id; do
        echo "Удаляем операцию: $operation_id"
        {{ ydb-cli }} operation forget "$operation_id"
    done
}
```

## Мониторинг статуса и прогресса

### Ожидание завершения операции

```bash
# Функция ожидания завершения операции резервного копирования
wait_for_backup_completion() {
    local operation_id="$1"
    local timeout_seconds="${2:-3600}"  # По умолчанию 1 час
    local elapsed=0

    echo "Ожидаем завершения операции: $operation_id"

    while [ $elapsed -lt $timeout_seconds ]; do
        status=$({{ ydb-cli }} operation get "$operation_id" --format proto-json-base64 | jq -r '.ready')

        if [ "$status" = "true" ]; then
            echo "Операция завершена: $operation_id"
            {{ ydb-cli }} operation get "$operation_id" --format pretty
            return 0
        fi

        echo "Операция в процессе... (прошло ${elapsed}s)"
        sleep 30
        elapsed=$((elapsed + 30))
    done

    echo "TIMEOUT: Операция не завершилась за ${timeout_seconds} секунд"
    return 1
}

### Периодический мониторинг

```bash
# Скрипт для периодического мониторинга всех операций
monitor_all_operations() {
    while true; do
        echo "=== $(date) ==="
        echo "Активные операции резервного копирования:"
        {{ ydb-cli }} operation list incbackup --format pretty | head -20

        echo ""
        echo "Активные операции восстановления:"
        {{ ydb-cli }} operation list restore --format pretty | head -20

        echo ""
        echo "Ожидание 60 секунд..."
        sleep 60
    done
}
```

## Практические сценарии

### Сценарий 1: Мониторинг ежедневного бэкапа

```bash
#!/bin/bash
# daily_backup_monitor.sh

COLLECTION_NAME="daily_backup"
LOG_FILE="/var/log/daily_backup.log"

echo "$(date): Запуск ежедневного резервного копирования" >> $LOG_FILE

# Создание инкрементального бэкапа
{{ ydb-cli }} yql -s "BACKUP \`${COLLECTION_NAME}\` INCREMENTAL;"

# Получение ID операции
sleep 5
OPERATION_ID=$({{ ydb-cli }} operation list incbackup --format proto-json-base64 | jq -r '.operations[0].id')

if [ ! -z "$OPERATION_ID" ]; then
    echo "$(date): Операция запущена: $OPERATION_ID" >> $LOG_FILE

    # Ожидание завершения
    wait_for_backup_completion "$OPERATION_ID" 1800  # 30 минут timeout

    if [ $? -eq 0 ]; then
        echo "$(date): Резервное копирование успешно завершено" >> $LOG_FILE
        {{ ydb-cli }} operation forget "$OPERATION_ID"
    else
        echo "$(date): ОШИБКА: Резервное копирование не завершилось" >> $LOG_FILE
    fi
else
    echo "$(date): ОШИБКА: Не удалось получить ID операции" >> $LOG_FILE
fi

### Сценарий 2: Мониторинг восстановления после сбоя

```bash
#!/bin/bash
# disaster_recovery_monitor.sh

RESTORE_COLLECTION="recovery_backup"
LOG_FILE="/var/log/disaster_recovery.log"

echo "$(date): Начало процедуры восстановления после сбоя" >> $LOG_FILE

# Запуск восстановления (предполагается, что экспорт уже выполнен)
# ydb tools restore -p ".backups/collections/$RESTORE_COLLECTION/full_backup" -i "recovery_export/full_backup"

# Отслеживание операций восстановления
while true; do
    RESTORE_OPERATIONS=$({{ ydb-cli }} operation list restore --format proto-json-base64 | jq -r '.operations[] | select(.ready==false) | .id')

    if [ -z "$RESTORE_OPERATIONS" ]; then
        echo "$(date): Все операции восстановления завершены" >> $LOG_FILE
        break
    fi

    echo "$RESTORE_OPERATIONS" | while read operation_id; do
        echo "$(date): Мониторинг операции восстановления: $operation_id" >> $LOG_FILE
        {{ ydb-cli }} operation get "$operation_id" --format pretty >> $LOG_FILE
    done

    sleep 60
done

echo "$(date): Процедура восстановления завершена" >> $LOG_FILE
```

## Лучшие практики мониторинга

### Рекомендации по периодичности проверок

1. **Ежедневные бэкапы**: Проверять статус каждые 15-30 минут
2. **Еженедельные полные бэкапы**: Проверять каждые 5-10 минут из-за большего объема
3. **Операции восстановления**: Проверять каждые 2-5 минут

### Настройка уведомлений

```bash
# Пример интеграции с системой уведомлений
notify_on_failure() {
    local operation_id="$1"
    local status=$({{ ydb-cli }} operation get "$operation_id" --format proto-json-base64 | jq -r '.status')

    if [ "$status" = "FAILED" ]; then
        # Отправка уведомления (настройте под вашу систему)
        echo "CRITICAL: Операция $operation_id завершилась с ошибкой" | \
        mail -s "YDB Backup Failure" admin@company.com
    fi
}
```

### Очистка и обслуживание

```bash
# Еженедельная очистка завершенных операций
weekly_cleanup() {
    echo "Еженедельная очистка операций: $(date)"

    cleanup_backup_operations
    cleanup_restore_operations

    echo "Очистка завершена: $(date)"
}
```

## См. также

- [SQL API](sql-api.md) - Справочник команд для создания бэкапов
- [Создание коллекций](create-collection.md) - Настройка и создание коллекций
- [Инкрементальные бэкапы](incremental-backups.md) - Работа с инкрементальными копиями
- [Восстановление](restore-from-collection.md) - Процедуры восстановления данных
