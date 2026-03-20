# Обработка событий с одним ключом в строгом порядке

## Проблема

Часто при обработке сообщений в топике нам важно, чтобы все сообщения с одним ключом были обработаны в строгой последовательности. Например, при обработке банковских операций, мы должны обработать пополнение счета и снятие с него в строгом порядке иначе пользователь будет разочарован. 

## Решение

### 1. Используйте пул писателей по количеству партиций и хэш ключа

1. Создайте пул писателей по количеству партиций
2. При записи сообщения вычислите хеш от его ключа
3. Выберите писателя из пула по модулю от хеша ключа на количество партиций
4. Не меняйте количество партиций в топике

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

#### ✅ Хороший пример (шардирование по пользователям)

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

### 2. Используйте Kafka API

В Kafka API все сообщения с одинаковым ключом попадают в одну партицию (при неизменном количестве партиций) и тем самым гарантируется порядок обработки сообщений с одинаковым ключом.

Начало работы с Kafka API: [{#T}](../../reference/kafka-api/examples)

Ограничения Kafka API: [{#T}](../../reference/kafka-api/constraints)
