# Включение SQS API для {{ ydb-short-name }} Topics

{{ ydb-short-name }} поддерживает доступ к [топикам](../../concepts/datamodel/topic.md) по протоколу [SQS](https://en.wikipedia.org/wiki/Amazon_Simple_Queue_Service).

Для включения этой возможности начиная с {{ ydb-short-name }} версии 26.1 необходимо:

- включить флаг [`enable_topic_message_level_parallelism`](../configuration/feature_flags.md);
- сконфигурировать [`http_proxy_config`](../configuration/http_proxy_config.md).
