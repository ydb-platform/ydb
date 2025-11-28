#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Использование: $0 <PID процесса>"
    exit 1
fi

PID=$1
LOG_FILE="memory_usage.log"

# Проверка существования процесса
if ! ps -p "$PID" > /dev/null; then
    echo "Процесс с PID $PID не найден!"
    exit 1
fi

echo "Мониторинг RSS и VIRT для PID $PID..."
echo "Время                PID   RSS (KB)  VIRT (KB)" > "$LOG_FILE"

while ps -p "$PID" > /dev/null; do
    # Получаем текущее время
    timestamp=$(date +"%Y-%m-%d_%H:%M:%S")
    
    # Извлекаем значения памяти
    mem_info=$(ps -o rss=,vsize= -p "$PID")
    
    # Записываем данные в файл
    echo "$timestamp $PID $mem_info" | awk '{printf "%-20s %-6s %-9s %-10s\n", $1, $2, $3, $4}' >> "$LOG_FILE"
    
    # Пауза 1 секунда
    sleep 1
done

echo "Процесс $PID завершен. Данные сохранены в $LOG_FILE"

