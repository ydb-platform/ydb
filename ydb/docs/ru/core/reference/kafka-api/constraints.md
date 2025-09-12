# Ограничения Kafka API

Поддержка протокола Kafka версии 3.4.0 осуществляется в ограниченном объеме:

1. Поддержана только [SASL/PLAIN-аутентификация](https://kafka.apache.org/documentation/#security_sasl).
1. Не поддержано [сжатие сообщений](https://www.confluent.io/blog/apache-kafka-message-compression/).
1. Не поддержана [операция удаления топика](https://kafka.apache.org/protocol#The_Messages_DeleteTopics). Для удаления топика используйте [YQL](../../yql/reference/syntax/drop-topic.md) или [{{ ydb-short-name }} CLI](../ydb-cli/topic-drop.md).
1. Не поддержана [проверка crc](https://kafka.apache.org/documentation/#consumerconfigs_check.crcs).
1. Не поддержана [работа с ACL](https://kafka.apache.org/documentation/#security_authz). Для управления доступом к топикам используйте [YQL](../../yql/reference/syntax/grant.md).
1. Если на топике включено [автопартиционирование](../../concepts/datamodel/topic.md#autopartitioning), то в такой топик нельзя писать или читать из него по протоколу Kafka API.
