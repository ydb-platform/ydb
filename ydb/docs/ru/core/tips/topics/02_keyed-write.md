# Обработка событий с одним ключом в строгом порядке

## Проблема

Во многих сценариях важно, чтобы все сообщения с одним ключом обрабатывались строго по порядку. Например, для операций по одному счёту сначала должно обработаться пополнение, а уже затем списание.

## Решение

### 1. Используйте пул писателей по количеству партиций и хэш ключа

1. Создайте пул писателей по числу партиций.
2. Для каждого сообщения вычисляйте хэш его ключа.
3. Выбирайте писателя по остатку от деления хэша на количество партиций.
4. Не меняйте количество партиций, если хотите сохранить то же распределение ключей.

#### ❌ Плохой пример (все ключи окажутся в одной партиции)

```java
// Все пользователи пишут через один producerId
public class UserEventService {
    private final SyncWriter writer;
    
    public UserEventService(TopicClient topicClient) {
        WriterSettings settings = WriterSettings.newBuilder()
            .setTopicPath("user-events")
            .setProducerId("user-service")
            .setMessageGroupId("user-service")
            .build();
        this.writer = topicClient.createSyncWriter(settings);
    }
    
    public void sendUserEvent(String userId, byte[] eventData) {
        // Все события идут через одну сессию
        writer.write(Message.of(eventData));
    }
}
```

#### ✅ Хороший пример: распределение по ключу пользователя

```java
public class ShardedEventService {
    private final Map<String, SyncWriter> writers;
    private final int partitionsCount;
    
    public ShardedEventService(TopicClient topicClient, int partitionsCount) {
        this.partitionsCount = partitionsCount;
        this.writers = new HashMap<>();
        
        for (int i = 0; i < partitionsCount; i++) {
            String producerId = "user-service-part-" + i;
            WriterSettings settings = WriterSettings.newBuilder()
                .setTopicPath("user-events")
                .setProducerId(producerId)
                .build();
            writers.put(producerId, topicClient.createSyncWriter(settings));
        }
    }
    
    public void sendUserEvent(String userId, byte[] eventData) {
        String producerId = selectProducerId(userId);
        writers.get(producerId).write(Message.of(eventData));
    }
    
    private String selectProducerId(String userId) {
        int shardIndex = Math.abs(userId.hashCode()) % partitionsCount;
        return "user-service-part-" + shardIndex;
    }
}
```

### 2. Если нужна маршрутизация по ключу из коробки, используйте Kafka API

В Kafka API сообщения с одинаковым ключом попадают в одну партицию, если количество партиций не меняется. Это позволяет сохранить порядок обработки для каждого ключа без собственной логики выбора партиции на клиенте.

Начало работы с Kafka API: [{#T}](../../reference/kafka-api/examples)

Ограничения Kafka API: [{#T}](../../reference/kafka-api/constraints)
