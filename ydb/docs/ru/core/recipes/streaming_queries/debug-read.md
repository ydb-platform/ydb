# Отладочное чтение из топика

При разработке [потоковых запросов](../../concepts/streaming-query.md) бывает полезно быстро посмотреть, какие данные поступают в [топик](../../concepts/datamodel/topic.md), без создания полноценного потокового запроса. Для этого выполните обычный `SELECT` с параметром `STREAMING = TRUE`.

Описание чтения из топика — в статье [{#T}](../../concepts/query_execution/topics.md):

- [Потоковое чтение](../../concepts/query_execution/topics.md#streaming-read) — чтение новых сообщений с `STREAMING = TRUE`;
- [Формат и схема сообщений](../../concepts/query_execution/topics.md#format-schema) — разбор JSON и других форматов;
- [Локальные и внешние топики](../../concepts/query_execution/local-and-external-topics.md) — обращение к топикам текущей и другой базы.

{% note warning %}

Потоковое чтение через `SELECT` предназначено только для отладки. Для промышленного использования создавайте [потоковые запросы](../../concepts/streaming-query.md) через [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

## См. также

* [{#T}](../../concepts/query_execution/topics.md)
* [{#T}](../../yql/reference/syntax/select/streaming.md)
* [{#T}](../../dev/streaming-query/streaming-query-formats.md)
