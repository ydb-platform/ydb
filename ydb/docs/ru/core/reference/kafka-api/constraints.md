# Ограничения Kafka API

Поддержка протокола Kafka версии 3.4.0 осуществляется в ограниченном объеме:

1. Разрешены только аутентифицированные подключения.
2. Поддержана только SASL/PLAIN-аутентификация.
3. Не поддержано сжатие сообщений.
4. В чтении поддержан только Manual Partition Assignment, [метод assign](https://kafka.apache.org/35/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#assign(java.util.Collection)), без использования групп партиций консьюмеров.
5. Не поддержаны транзакции.
6. Не поддержаны DDL операции. Для осуществления DDL-операций пользуйтесь [YDB SDK](../ydb-sdk/index.md) или [YDB CLI](../ydb-cli/index.md).
7. Не поддержана проверка схемы данных.