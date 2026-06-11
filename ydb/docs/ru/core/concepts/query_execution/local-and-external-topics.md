# Локальные и внешние топики {#local-external-topics}

[YQL-запросы к топикам](topics.md) и [потоковые запросы](../streaming-query.md) читают события из [топиков](../datamodel/topic.md) и могут записывать результаты обратно в топики. Источником и приёмником сообщений может быть как топик **в той же базе данных**, в которой выполняется запрос, так и топик **в другой базе** {{ ydb-short-name }}.

Все сценарии использования работают одинаково для [локальных](#local-topics) и [внешних](#external-topics) топиков. Один и тот же запрос может одновременно читать локальный топик, писать во внешний и наоборот.

## Локальные топики {#local-topics}

**Локальные топики** — топики, созданные в **той же базе** {{ ydb-short-name }}, что и выполняемый запрос.

В тексте запроса к ним обращаются **по короткому имени** — так же, как к таблице в текущей базе:

```yql
SELECT * FROM input_topic WITH (FORMAT = json_each_row, SCHEMA = (...));
```

```yql
INSERT INTO output_topic SELECT ...;
```

## Внешние топики {#external-topics}

**Внешние топики** — топики, расположенные **в другой базе** {{ ydb-short-name }}.

Доступ к ним выполняется только через заранее созданный [внешний источник данных](../datamodel/external_data_source.md) с типом источника YDB. Создание объекта — команда [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md); при необходимости аутентификации используются [секреты](../../yql/reference/syntax/create-secret.md).

После создания источника, например с именем `ext_source`, обращение к топику `input_topic` во внешней базе записывается так:

```yql
SELECT * FROM ext_source.input_topic WITH (FORMAT = json_each_row, SCHEMA = (...));
```

Имя `ext_source` в документации **условное** — в вашей базе источник может называться иначе; важно, чтобы оно совпадало в `CREATE EXTERNAL DATA SOURCE` и в префиксе перед именем топика.

## См. также

- [YQL-запросы к топикам](topics.md)
- [Типичные шаблоны потоковых запросов](../../dev/streaming-query/patterns.md) — готовые фрагменты YQL
- [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md) — создание потокового запроса
- [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md) — объявление источника для внешней базы
- [Внешний источник данных](../datamodel/external_data_source.md)
- [Топик](../datamodel/topic.md)
- [Обогащение данных](../../dev/streaming-query/enrichment.md)
- [Чтение из топика](topics.md#topic-read)
