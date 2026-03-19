# Обработка событий с одним ключом в строгом порядке

## Проблема

Часто при обработке сообщений в топике нам важно, чтобы все сообщения с одним ключом были обработаны в строгой последовательности. Например, при обработке банковских операций, мы должны обработать пополнение счета и снятие с него в строгом порядке иначе пользователь будет разочарован. 

## Решение

Решение подробно описано тут: https://logbroker.yandex-team.ru/docs/faq#how-to-preserve-partitioning-for-the-same-key 

### 1. Используйте пулл писателей по количеству партиций и хэш ключа

1. Создайте пулл писателей по количеству партиций
2. При записи сообщения вычислите хеш от его ключа
3. Выберите писателя из пулла по модулю от хеша ключа на количество партиций
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

### 2. (Скоро) Используйте наш новый интерфейс в Go и C++ SDK: KeyedWriteSession

Голосуйте за задачу, если вы хотите воспользовать логикой выше, но не хотите писать ее сами: https://st.yandex-team.ru/LOGBROKER-10104

### 3. Используйте Kafka API

В Kafka API все сообщения с одинаковым ключом попадают в одну партицию (при неизменном количестве партиций) и тем самым гарантируется порядок обработки сообщений с одинаковым ключом.

Начало работы с Kafka API: https://logbroker.yandex-team.ru/docs/how_to/using_kafka_api

Ограничения Kafka API: https://ydb.yandex-team.ru/docs/reference/kafka-api/constraints
