# Ограничения Kafka API

Поддержка протокола Kafka версии 3.4.0 осуществляется в ограниченном объеме:

1. Поддержана только SASL/PLAIN-аутентификация.
1. Не поддержаны [топики с настройкой `cleanup.policy=compact`](https://docs.confluent.io/kafka/design/log_compaction.html).
В связи с этим поверх Kafka API в YDB Topics не работают Kafka Connect, Schema Registry и Kafka Streams.
1. Не поддержано сжатие сообщений.
1. Не поддержаны транзакции.
1. Не поддержаны DDL операции. Для осуществления DDL-операций пользуйтесь [{{ ydb-short-name }} SDK](../ydb-sdk/index.md) или [{{ ydb-short-name }} CLI](../ydb-cli/index.md).
1. Не поддержана проверка crc.
1. Работа Kafka Connect поддерживается только в режиме одного процесса-исполнителя (standalone mode).
1. Если на топике включено автопартиционирование, то в такой топик нельзя писать или читать по протоколу Kafka API.
