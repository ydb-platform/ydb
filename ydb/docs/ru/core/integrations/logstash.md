# Настройка интеграции между Logstash и {{ ydb-short-name }}

В разделе представлены варианты интеграции между системой сбора и анализа данных Logstash и {{ ydb-short-name }}.

## Введение

[Logstash](https://www.elastic.co/logstash) – инструмент сбора, фильтрации и нормализации логов. Данный продукт позволяет динамически получать, фильтровать и доставлять данные независимо от их формата и сложности. Конфигурация системы производиться с помощью плагинов различных типов - input, output и filter. Набор Logstash плагинов для работы с {{ ydb-short-name }} располагается в репозитории [ydb-logstash-plugins](https://github.com/ydb-platform/ydb-logstash-plugins):

* Storage Plugin для сохранения данных в [строчной](../concepts/datamodel/table.md#strokovye-tablicy) или [колоночной](../concepts/datamodel/table.md#column-tables) таблице {{ ydb-short-name }};
* Input Topic Plugin для чтение данных из {{ ydb-short-name }} [топика](../concepts/topic.md);
* Output Topic Plugin для отправки данных в {{ ydb-short-name }} [топик](../concepts/topic.md).

Плагины можно [собрать](https://github.com/ydb-platform/ydb-logstash-plugins/blob/main/BUILD.md) самостоятельно из исходного кода, либо воспользоваться готовыми [сборками](https://github.com/ydb-platform/ydb-logstash-plugins/releases) под две последнии версии Logstash.

{% note info %}

Дальнейшие инструкции используют placeholder `<path-to-logstash>`, который нужно заменить на путь до установленного в системе Logstash.

{% endnote %}

Установить любой собранный плагин в Logstash можно с помощью команды
```bash
<path-to-logstash>/bin/logstash-plugin install </path/to/logstash-plugin.gem>
```

Проверить что плагин установлен, можно командой
```bash
<path-to-logstash>/bin/logstash-plugin list
```
Которая выведет список всех установленных плагинов, в котором можно найти интересующие.

## Конфигурация подключения плагинов к {{ ydb-short-name }}

Все плагины используют одни и те же параметры для настройки подключения к базе данных {{ ydb-short-name }}. Среди этих параметров обязателен только `connection_string`, а все остальные параметры опциональны и позволяют задать [режим аутентификации](../concepts/auth.md). Если ни один из них не указан, то будет использоваться анонимная аутентификация.

```ruby
# Пример указан для плагина ydb_storage, настройка подключения
# в плагинах ydb_topics_output и ydb_topics_input аналогична
ydb_storage {
    # Строка подключения к БД, содержит схему, адрес, порт и путь БД
    connection_string => "grpc://localhost:2136/local"
    # Значение токена аутентификации (режим Access Token)
    token_auth => "<token_value>"
    # Путь до файла со значением токена аутентификации (режим Access Token)
    token_file => "</path/to/token/file>"
    # Путь до ключа сервисного аккаунта (режим Service Account Key)
    sa_key_file => "</path/to/key.json>"
    # Флаг для использования сервиса метаданных (режим Metadata)
    use_metadata => true
}
```

## Плагин {{ ydb-short-name }} Storage

Данный плагин позволяет перенаправить поток данных из Logstash в таблицу {{ ydb-short-name }} для дальнейшего анализа. Для этого удобно использовать [колоночные таблицы](../concepts/datamodel/table.md#column-tables), оптимизированные для аналитических запросов. Каждое отдельное [поле события Logstash](https://www.elastic.co/guide/en/logstash/current/event-dependent-configuration.html) будет записано в отдельный столбец таблицы. Tе поля, которые не соответствуют ни одной колонке, будут проигнорированы.

### Конфигурация плагина

Для настройки плагина необходимо добавить секцию `ydb_storage` в раздел `output` файла конфигурации [Logstash](https://www.elastic.co/guide/en/logstash/current/configuration.html). Плагин поддерживает стандартный набор опций для [подключения плагинов {{ ydb-short-name }}](#konfiguraciya-podklyucheniya-plaginov-k-ydb), плюс несколько специфичных для него опций:

* `table_name` — обязательный параметр с именем таблицы, в которую будут записываться события Logstash
* `uuid_column` — необязательный параметр с именем колонки, в которую плагин будет записывать случайно сгенерированный идентификатор
* `timestamp_column` — необязательный параметр с именем колонку для записи времени создания события Logstash

{% note warning %}

Плагин Storage не проверяет события Logstash с точки зрения корректности и уникальности первичных ключей таблицы {{ ydb-short-name }}. Для записи в {{ ydb-short-name }} используется [пакетная загрузка данных](../dev/batch-upload.md) и при получении различных событий с одинаковыми идентификаторами они будут перезаписывать друг друга. Рекомендуется выбирать в качестве первичные ключей те поля события, которые будут присутствовать в каждом сообщении и однозначно его идентифицировать. Если же такого набора полей нет, то можно добавить в первичный ключ специальную колонку и заполнять ее случайным значением с помощью параметра `uuid_column`

{% endnote %}

### Пример использования

#### Создание колоночной таблицы для хранения записей
В выбранной базе данных {{ ydb-short-name }} cоздадим новую колоночную таблицу с нужными нами колонками. В качестве первичного ключа будем использовать случайное значение, генерируемое на стороне плагина:

```sql
CREATE TABLE `logstash_demo` (
    `uuid`     Text NOT NULL,      -- Уникальный идентификатор
    `ts`       Timestamp NOT NULL, -- Время создания сообщения
    `message`  Text,           
    `user`     Text,           
    `level`    Int32,

    PRIMARY KEY (
         `uuid`
    )
)
WITH (STORE = COLUMN);
```

#### Настройка плагина в Logstash

Добавим секцию плагина `ydb_storage` в раздел `output` файла конфигурации [Logstash](https://www.elastic.co/guide/en/logstash/current/configuration.html). Дополнительно для проверки работоспособности также добавим плагин `http` в секцию `input`, что позволит создавать события в `Logstash` через http запросы:

```ruby
output {
  ydb_storage {
    connection_string => "..."    # Адрес подключения к  {{ ydb-short-name }}
    table_name => "logstash_demo" # Имя таблицы
    uuid_column => "uuid"         # Колонка в которую плагин будет записывать сгенерированный идентификатор
    timestamp_column => "ts"      # Колонку в которую плагин будет записывать timestamp события
  }
}

input {
  http {
    port => 9876 # Можно указать любой порт
  }
}
```
После изменения данного файла `Logstash` нужно перезапустить.

#### Отправка тестовых сообщений
Затем можно отправить несколько тестовых сообщений:
```bash
curl -H "content-type: application/json" -XPUT 'http://127.0.0.1:9876/http/ping' -d '{ "user" : "demo_user", "message" : "demo message", "level": 4}'
curl -H "content-type: application/json" -XPUT 'http://127.0.0.1:9876/http/ping' -d '{ "user" : "test1", "level": 1}'
curl -H "content-type: application/json" -XPUT 'http://127.0.0.1:9876/http/ping' -d '{ "message" : "error", "level": -3}'
```
Все команды должны вернуть `ok` в случае успешного выполнения.

#### Проверка наличия записанных сообщений в {{ ydb-short-name }} 
Теперь можно убедиться что все отправленные сообщения записаны в таблице. Выполним запрос (не забывая что чтение из колоночной таблицы возможно только в [режиме ScanQuery](../reference/ydb-cli/commands/scan-query.md)):
```sql
SELECT * FROM `logstash_demo`;
```
и получим список записанных событий:
```
┌───────┬────────────────┬───────────────────────────────┬─────────────┬────────────────────────────────────────┐
│ level │ message        │ ts                            │  user       │ uuid                                   │
├───────┼────────────────┼───────────────────────────────┼─────────────┼────────────────────────────────────────┤
│ -3    │ "error"        │ "2024-05-22T13:16:06.491000Z" │  null       │ "74cd4048-0b61-4fb9-9385-308714e21881" │
│  1    │ null           │ "2024-05-22T13:15:56.591000Z" │ "test1"     │ "1df27d0a-9aa0-42c7-9ea2-ab69bc1f5d87" │
│  4    │ "demo message" │ "2024-05-22T13:15:38.760000Z" │ "demo_user" │ "b7468cb1-e1e3-46fa-965d-83e604e80a31" │
└───────┴────────────────┴───────────────────────────────┴─────────────┴────────────────────────────────────────┘
```


## Плагин {{ ydb-short-name }} Topic Input

Данный плагин позволяет читать из {{ ydb-short-name }} [топика](../concepts/topic.md) и преобразовывать их в события `Logstash` для дальнейшей обработки.

### Конфигурация плагина

Для настройки плагина мы должны добавить секцию `ydb_topic` в раздел `input` файла конфигурации [Logstash](https://www.elastic.co/guide/en/logstash/current/configuration.html). Плагин поддерживает стандартный набор опций для [подключения плагинов {{ ydb-short-name }}](#konfiguraciya-podklyucheniya-plaginov-k-ydb), плюс несколько специфичных для него опций:

* `topic_path` — обязательный параметр с полным путем топика для чтения;
* `consumer_name` — обязательный параметр с именем [читателя](../concepts/topic.md#consumer) топика;
* `schema` — необязательный параметр с вариантам обработки сообщений {{ ydb-short-name }}. По умолчанию плагин читает и отправляет сообщения топика в бинарном виде, но если указать режим `JSON`, то каждое сообщение из топика будет трактоваться как JSON объект.

### Пример использования

#### Создание топика
В выбранной базе данных {{ ydb-short-name }} заранее создадим топик и читателя, из которого будет производиться чтение:

```bash
ydb -e grpc://localhost:2136 -d /local topic create /local/logstash_demo_topic
ydb -e grpc://localhost:2136 -d /local topic consumer add --consumer logstash-consumer /local/logstash_demo_topic
```

#### Настройка плагина в Logstash

Добавим секцию плагина `ydb_topic` в раздел `input` файла конфигурации [Logstash](https://www.elastic.co/guide/en/logstash/current/configuration.html). Дополнительно для проверки работоспособности также добавим плагин `stdout` в секцию `output`, что позволит отслеживать все события в логе выполнения `Logstash`:

```ruby
input {
  ydb_topic {
    connection_string => "grpc://localhost:2136/local" # Адрес подключения к {{ ydb-short-name }}
    topic_path => "/local/logstash_demo_topic"         # Имя топика
    consumer_name => "logstash-consumer"               # Имя читателя
    schema => "JSON"                                   # Используем JSON в качестве формата сообщений топика
  }
}

output {
  stdout {
    codec => rubydebug
  }
}
```
После изменения данного файла `Logstash` нужно перезапустить.

#### Тестовая запись сообщение в топик
Затем отправим несколько тестовых сообщений:
```bash
echo '{"message":"test"}' | ydb -e grpc://localhost:2136 -d /local topic write /local/logstash_demo_topic
echo '{"user":123}' | ydb -e grpc://localhost:2136 -d /local topic write /local/logstash_demo_topic
```

#### Проверка обработки сообщений в Logstash

Благодаря включенному плагину `stdout` в логе Logstash появятся прочитанные из топика сообщения:
```
{
       "message" => "test",
    "@timestamp" => 2024-05-23T10:31:47.712896899Z,
      "@version" => "1"
}
{
          "user" => 123.0,
    "@timestamp" => 2024-05-23T10:34:08.574599108Z,
      "@version" => "1"
}
```


## Плагин {{ ydb-short-name }} Topic Output

Данный плагин позволяет записывать события  `Logstash` в {{ ydb-short-name }} [топик](../concepts/topic.md).

### Конфигурация плагина

Для настройки плагина мы должны добавить секцию `ydb_topic` в раздел `output` файла конфигурации [Logstash](https://www.elastic.co/guide/en/logstash/current/configuration.html). Плагин поддерживает стандартный набор опций для [подключения плагинов {{ ydb-short-name }}](#konfiguraciya-podklyucheniya-plaginov-k-ydb), плюс дополнительные опции:

* `topic_path` — обязательный параметр с полным путем топика для записи.

### Пример использования

#### Создание топика
В выбранной базе данных {{ ydb-short-name }} заранее создадим топик, в который будет производиться запись:

```bash
ydb -e grpc://localhost:2136 -d /local topic create /local/logstash_demo_topic
```

#### Настройка плагина в Logstash

Добавим секцию плагина `ydb_topic` в раздел `output` файла конфигурации [Logstash](https://www.elastic.co/guide/en/logstash/current/configuration.html).. Дополнительно для проверки работоспособности также добавим плагин `http` в секцию `input`, что позволит создавать события в `Logstash` через http запросы:

```ruby
output {
  ydb_topic {
    connection_string => "grpc://localhost:2136/local" # Адрес подключения к  {{ ydb-short-name }}
    topic_path => "/local/logstash_demo_topic"         # Имя топика для записи
  }
}

input {
  http {
    port => 9876 # Можно указать любой порт
  }
}
```
После изменения данного файла `Logstash` нужно перезапустить.

#### Отправка тестового сообщения
С помощью плагина `http` отправим тестовое сообщение:
```bash
curl -H "content-type: application/json" -XPUT 'http://127.0.0.1:9876/http/ping' -d '{ "user" : "demo_user", "message" : "demo message", "level": 4}'
```
Команда вернет `ok` в случае успешного выполнения.

#### Контрольное чтение сообщения из топика
Для проверки успешности записи в топик создадим нового читателя и прочитаем одно сообщение из топика:
```bash
ydb -e grpc://localhost:2136 -d /local topic consumer add --consumer logstash-consumer /local/logstash_demo_topic
ydb -e grpc://localhost:2136 -d /local topic read /local/logstash_demo_topic --consumer logstash-consumer --commit true
```
Чтение вернет содержимое отправленного сообщения:
```json
{"level":4,"message":"demo message","timestamp":1716470292640,"user":"demo_user"}
```
