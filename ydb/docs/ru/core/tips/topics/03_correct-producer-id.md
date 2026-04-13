# Осмысленный producerId для отладки и производительности

## Проблема

Использование случайных или непредсказуемых `producerId` в {{ ydb-short-name }} Topics приводит к нескольким серьезным проблемам:

1. **Ухудшение производительности** - {{ ydb-short-name }} хранит информацию о всех `producerId` за последние несколько недель для обеспечения сохранения номеров партиций. Частые пересоздания сессий с новыми случайными `producerId` создают дополнительную нагрузку на систему.

2. **Конфликты сессий** - при параллельной записи из нескольких экземпляров приложения с одинаковым `producerId` возникают ошибки типа "ownership session is killed by another session".
  
3. **Сложность отладки и трассировки** - случайные идентификаторы затрудняют сопоставление сообщений с конкретным сервисом или инстансом, откуда они были отправлены.

## Решение

### Принципы правильного использования producerId

1. **Используйте осмысленные идентификаторы** - комбинация имени сервиса, идентификатора инстанса и номера партиции
2. **Обеспечьте уникальность в рамках партиции** - для записи в разные партиции используйте разные `producerId`
3. **Минимизируйте количество уникальных producerId** - не создавайте новые идентификаторы для каждого запроса
4. **Используйте стабильные идентификаторы** - переиспользуйте `producerId` между перезапусками сервиса

### Рекомендуемые стратегии

- Для сервисов с одним инстансом: `имя_сервиса-номер_партиции`
- Для сервисов с несколькими инстансами: `имя_сервиса-идентификатор_инстанса-номер_партиции`


### Пример плохой практики

```python
# НЕПРАВИЛЬНО - случайный producerId для каждого запроса
import uuid
import ydb

def write_to_topic_bad():
    driver = ydb.Driver(...)
    
    # Каждый раз новый случайный producerId
    producer_id = str(uuid.uuid4())
    
    writer = driver.topic_client.writer(
        "my-topic",
        producer_id=producer_id
    )
    
    # Запись сообщения
    writer.write("message data")
```

### Пример хорошей практики

```python
# ПРАВИЛЬНО - осмысленный и стабильный producerId
import socket
import ydb

def get_stable_producer_id(partition):
    """Генерирует стабильный producerId на основе имени хоста, названии сервиса и номера партиции"""
    hostname = socket.gethostname()
    service_name = "my-service"
    return f"{service_name}-{hostname}-{partition}"

def write_to_topic_good():
    driver = ydb.Driver(...)

    writers = {}
    for partition in range(partitions_count):
        # Стабильный producerId, переиспользуется между запросами
        producer_id = get_stable_producer_id(partition)
        
        writer = driver.topic_client.writer(
            "my-topic",
            producer_id=producer_id,
            message_group_id=producer_id
        )
        writers[partition] = writer
    
    
```

**Преимущества:**
- Легкая идентификация источника сообщений
- Минимальное количество уникальных producerId
- Оптимальная производительность системы

### Пример для распределенного сервиса

```go
// ХОРОШО - уникальные producerId для каждого воркера
package main

import (
    "fmt"
    "os"
    "github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
    "github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func getProducerID() string {
    hostname, _ := os.Hostname()
    workerID := os.Getenv("WORKER_ID")
    if workerID == "" {
        workerID = "default"
    }
    return fmt.Sprintf("analytics-service-%s-%s", hostname, workerID)
}

func main() {
    // Инициализация драйвера
    db, err := ydb.Open(ctx, ...)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    producerID := getProducerID()
    
    writer, err := db.Topic().StartWriter(
        producerID,
        "analytics-topic"
    )
    if err != nil {
        log.Fatal(err)
    }
    
    // Использование writer
}
```


## Ключевые выводы

1. **Используйте осмысленные идентификаторы** - это упрощает отладку и мониторинг
2. **Минимизируйте количество producerId** - переиспользуйте идентификаторы между сессиями
3. **Следите за лимитами** - мониторьте количество уникальных producerId на партицию с помощью метрики [SourceIdMaxCount](https://logbroker.yandex-team.ru/docs/reference/metrics#SourceIdMaxCount)
4. **Избегайте конфликтов** - для параллельных операций используйте разные идентификаторы
5. **Документируйте стратегию** - определите единый подход для всех сервисов команды
