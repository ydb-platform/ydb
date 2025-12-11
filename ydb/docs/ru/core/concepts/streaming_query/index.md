# Потоковые запросы

### Зачем нужны потоковые запросы

Потоковые запросы предназначены для обработки данных, которые поступают непрерывно, в реальном времени (а не лежат "мертвым грузом" в таблице базе данных). В обычных запросах вы сначала сохраняете данные, а потом запрашиваете их. В потоковых запросах вы сначала формулируете запрос, а данные "протекают" сквозь него.

Отличительные особенности от обычных запросов:

- мгновенная реакция. Традиционные отчеты строятся раз в день или раз в час. Потоковые запросы дают ответ через миллисекунды после события.
- работа с бесконечными данными. Многие источники данных никогда не заканчиваются: логи серверов, клики пользователей на сайте, показания датчиков IoT, финансовые тикеры.
- аналитика во времени. Например, вы хотите знать, сколько покупок было совершено с 12:00 до 13:00. Потоковый запрос может открыть "окно", подождать опоздавшие события, пересчитать результат и выдать точную цифру, даже если события пришли вразнобой.
- непрерывное обновление результатов. Например, вы хотите построить дашборд "Топ-10 продаваемых товаров за последние 10 минут". Потоковый запрос постоянно обновляет состояние этого топа.
- возможность сложной логики с сохранением состояния. Например, вы хотите отправить алерт, если пользователь совершил 3 неудачные попытки входа подряд за 5 минут.

### Технические особенности

Технические особенности потоковых запросов:

- не имеют максимальной длительности (таймаута),
- сами производят перезапуск в случае ошибок (падений нод, дисконнектов),
- периодически сохраняют свое состояние (чекпойнты),
- не могут иметь результата. Посчитанные данные следует явно вставить в другой топик.

На данный момент в качестве неограниченных входных данных могут быть использованы [топики](../topic). Для обогащения основного потока могут быть использованы ограниченные потоки (внешние источники S3). Для выходных данных могут быть использованы только топики.

В случае внутренних сбоев запрос автоматически перезапускается и восстанавливается из последнего сохраненного [чекпойнта](checkpoints.md).

Потоковые запросы обеспечивают гарантию at least once. Это гарантируется повторной обработкой данных с последнего сохраненного чекпойнта с дедупликацией при вставке в топик.

В частности это обеспечивается:

- гарантиями [топиков](../topic) (at-least-once при чтении / exactly-once при записи),
- сохранения [смещений](../topic#offset) во входных топиках,
- сохранения [порядковых номеров сообщений](../topic#seqno) для дедупликации в выходных топиках,
- сохранения стейтов агрегаций для тасок, содержащих потоковую агрегацию (таких как `GROUP BY HOP` / `MATCH_RECOGNIZE`).

### Использование [читателя](../datamodel/topic#consumer)

По умолчанию чтение из топика происходит [без использования читателя](../../reference/ydb-sdk/topic.md#no-consumer).
Чтобы использовать читателя необходимо предварительно его создать через [CLI](../../reference/ydb-cli/topic-consumer-add) или при создании топика с помощью [CREATE TOPIC](../../yql/reference/syntax/create-topic.md).
Далее указать его имя в тексте запроса через `PRAGMA pq.Consumer=my_consumer` (см. пример в [CREATE STREAMING QUERY](../../../yql/reference/syntax/create-streaming-query)).

### Состояние запроса

Запросы могут быть в 2-х состояних: запущен или остановлен. При этом в запущенном состоянии запрос может быть в нескольких статусах.
Подробную информацию о запросе можно получить через системную таблицу [streaming_queries](../../dev/system-views.md#streaming_queries).
Пример запроса:

```sql
SELECT Path, Status, Text, Run FROM `.sys/streaming_queries`;
```

### Поддерживаемые типы данных

Топики {{ ydb-short-name }} хранят неструктурированные данные. Поэтому при чтении необоходимо указывать формат и схему данных (см. [Форматы данных](formats.md)). Запись можно выполнять только в виде неструктурированных данных (например как строка или JSON).

### Конфигурирование

Функциональность включается установкой флагов `enable_external_data_sources` и `enable_streaming_queries` в конфигурации кластера.
Пример:

```yaml
feature_flags:
  enable_external_data_sources: true
  enable_streaming_queries: true
```

### Синтаксис

Чтение реализовано через [внешние источники данных](../datamodel/external_data_source), поэтому предварительно необходимо создать источник через [CREATE EXTERNAL DATA SOURCE](../../../yql/reference/syntax/create-external-data-source).

Пример:

```sql
CREATE EXTERNAL DATA SOURCE `streaming_test/ydb_source` WITH (
    SOURCE_TYPE = 'Ydb',
    LOCATION = 'localhost:2135',
    DATABASE_NAME = '/Root',
    AUTH_METHOD = 'NONE'
);
```

Управлять потоковыми запросами можно с помощью следующих конструкций SQL:

- [CREATE STREAMING QUERY](../../../yql/reference/syntax/create-streaming-query),
- [ALTER STREAMING QUERY](../../../yql/reference/syntax/alter-streaming-query),
- [DROP STREAMING QUERY](../../../yql/reference/syntax/drop-streaming-query).

### Чтение из топика без использования `CREATE STREAMING QUERY`

Читать из топика можно обычным запросом (без использования `CREATE STREAMING QUERY`). При этом необходимо задать ограничение на результат (чтобы запрос завершился). Обычно данная возможность полезна с целью отладки запроса с последующим запуском запроса с использованием `CREATE STREAMING QUERY`.

Пример:
```sql
SELECT 
    Data
FROM
    `streaming_test/ydb_source`.topic_name
LIMIT 1;
```

### Потоковая аггрегация

Агрегация данных в потоковом режиме возможно с помощью:

- [GROUP BY HOP](../../../yql/reference/syntax/select/group-by#group-by-hop),
- [MATCH_RECOGNIZE](../../../yql/reference/syntax/select/match_recognize).

Примеры запросов смотрите в [рецептах](../../recipes/streaming_queries/index.md).

### Обогащение данных (S3) {#enrichment} 

Обогащение данных (S3) возможно с помощью в через [внешние источники данных](../federated_query/s3/external_data_source).

{% cut "Пример запроса" %}

```sql
CREATE EXTERNAL DATA SOURCE `streaming_test/s3_source` WITH (
    SOURCE_TYPE = "ObjectStorage",
    LOCATION = "https://storage.yandexcloud.net/my_bucket_name/",
    AUTH_METHOD = "NONE"
);

CREATE STREAMING QUERY `streaming_test/query_name` AS
DO BEGIN
$parsed =
    SELECT
        *
    FROM`streaming_test/source_name`.`streaming_recipe/input_topic`
    WITH (
        FORMAT = 'json_each_row',
        SCHEMA = (time String NOT NULL, service_id UInt32 NOT NULL, message String NOT NULL)
    );

$lookup =
    SELECT
        service_id,
        name
    FROM
      `streaming_test/s3_source`.`lookupnica/services`
    WITH (
        FORMAT = "csv_with_names",
        SCHEMA =
        (
            service_id UInt32,
            name Utf8,
        )    
    );

$parsed = (
    SELECT
        lookup.name AS name,
        p.*
    FROM
        $parsed AS p
    LEFT JOIN
        $lookup AS lookup
    ON
        (lookup.service_id = p.service_id)
    );


INSERT INTO `streaming_test/source_name`.`streaming_recipe/output_topic`
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $parsed
;
END DO;
```
{% endcut %}

На данный момент `JOIN` потока с таблицами {{ ydb-short-name }} (как локальными, так и внешними) не поддерживается (в разработке).

### См. также

- [Форматы данных](formats.md)
- [Чекпойнты](checkpoints.md)
- [Рецепты работы с потоковыми запросами](../../recipes/streaming_queries/index.md)
