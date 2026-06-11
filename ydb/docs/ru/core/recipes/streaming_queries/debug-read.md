# Отладочное чтение из топика

При разработке [потоковых запросов](../../concepts/streaming-query.md) бывает полезно быстро посмотреть, какие данные поступают в [топик](../../concepts/datamodel/topic.md), без создания полноценного потокового запроса. Для этого выполните обычный `SELECT` из топика.

Подробное описание чтения из топика — в статье [{#T}](../../concepts/query_execution/topics.md).

{% note warning %}

Чтение через `SELECT` предназначено только для отладки. Для промышленного использования создавайте [потоковые запросы](../../concepts/streaming-query.md) через [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

## См. также

* [{#T}](../../concepts/query_execution/topics.md)
* [{#T}](../../yql/reference/syntax/select/streaming.md)
* [{#T}](../../dev/streaming-query/streaming-query-formats.md)
