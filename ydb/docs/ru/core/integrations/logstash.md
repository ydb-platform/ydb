# Настройка интеграции между Logstash и {{ ydb-short-name }}

В разделе представлены варианты интеграции между системой сбора и анализа данных Logstash и {{ ydb-short-name }}.

## Введение

[Logstash](https://www.elastic.co/logstash) – инструмент сбора, фильтрации и нормализации логов. Данный продукт позволяет динамически получать, фильтровать
и доставлять данные независимо от их формата и сложности. Конфигурация системы производиться с помощью плагинов различных типов - input, output и filter.

Для работы с Logstash мы подготовили [набор плагинов](https://github.com/ydb-platform/ydb-logstash-plugins)

* Storage Plugin для сохранения данных в [строчной](../concepts/datamodel/table.md#strokovye-tablicy) или [колоночной](../concepts/datamodel/table.md#column-tables) таблице {{ ydb-short-name }} 
* Input Topic Plugin для чтение данных из {{ ydb-short-name }} [топика](../concepts/topic.md)
* Output Topic Plugin для отправки данных в {{ ydb-short-name }} [топик](../concepts/topic.md)

Плагины можно [собрать](https://github.com/ydb-platform/ydb-logstash-plugins/blob/main/BUILD.md) самостоятельно из исходного, кода либо воспользоваться готовыми [сборками](https://github.com/ydb-platform/ydb-logstash-plugins/releases) под две последнии версии Logstash

{% note info %}

Все дальнейшие инструкции используют placeholder `<path-to-logstash>`, который нужно заменить на путь до установленного в системе Logstash

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

## Плагин {{ ydb-short-name }} Storage

Данный плагин позволяет перенаправить поток данных из Logstash в таблицу {{ ydb-short-name }} для дальнейшего анализа. Для этого удобно использовать [колоночные таблицы](../concepts/datamodel/table.md#column-tables), оптимизированные для аналитических запросов. Каждое отдельное [поле события Logstash](https://www.elastic.co/guide/en/logstash/current/event-dependent-configuration.html) будет записано в отдельный столбец таблицы, те же поля, что не соответствуют ни одной колонке - будут проигнорированы.

### Конфигурация плагина

Для настройки плагина мы должны добавить секцию `ydb_storage` в раздел `output` файла конфигурации [Logstash](https://www.elastic.co/guide/en/logstash/current/configuration.html). Плагин поддерживает стандартный набор опций для [подключения плагинов {{ ydb-short-name }}](#konfiguraciya-podklyucheniya-plaginov-k-ydb), плюс несколько специфичных для него опций

* `table_name` - обязательный параметр с именем таблицы, в которую будут записываться события Logstash
* `uuid_column` - необязательный параметр с именем колонки, в которую плагин будет записывать случайно сгенерированный идентификатор
* `timestamp_column` - необязательный параметр с именем колонку для записи времени создания события Logstash

{% note warning %}

Плагин Storage не проверяет события Logstash с точки зрения корректности и уникальности первичных ключей таблицы {{ ydb-short-name }}. Для записи в {{ ydb-short-name }} используется [пакетная загрузка данных](../dev/batch-upload.md) и при получении различных событий с одинаковыми идентификаторами - они будут перезаписывать друг друга. Рекомендуется выбирать в качестве первичные ключей те поля события, которые будут присутствовать в каждом сообщении и однозначно его идентифицировать. Если же такого набора полей нет, то можно добавить в первичный ключ специальную колонку и заполнять ее случайным значением с помощью параметра `uuid_column`

{% endnote %}

### Пример использования

#### Создание колоночной таблицы для хранения записей
В выбранной базе данных {{ ydb-short-name }} мы можем создать новую колоночную таблицу с нужными нами колонками. В качестве первичного ключа будем использовать случайное значение, генерируемое на стороне плагина

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

Добавим секцию плагина `ydb_storage` в раздел `output` файла конфигурации [Logstash](https://www.elastic.co/guide/en/logstash/current/configuration.html).
Для тестирования работоспособности также добавим плагин `http` в секцию `input`, что позволит создавать события в `Logstash` через http запроса

```ruby
output {
  ydb_storage {
    connection_string => "grpc://localhost:2136/local" # Адрес подключения к  {{ ydb-short-name }}
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

#### Отправка и проверка тестового сообщения
Затем можно отправить тестовое сообщение 
```bash
curl -H "content-type: application/json" -XPUT 'http://127.0.0.1:9876/http/ping' -d '{
    "user" : "demo_user",
    "message" : "demo message",
    "level": 4
}'
```
и убедиться что данное сообщение записано в таблицу
```sql
SELECT * FROM `logstash_demo`;
```

## Использование плагина Input Topic
{% note info %}

Материал дополняется

{% endnote %}

## Использование плагина Output Topic
{% note info %}

Материал дополняется

{% endnote %}

## Конфигурация подключения плагинов к {{ ydb-short-name }}

Все плагины используют одни и те же параметры для настройки подключения к базе данных {{ ydb-short-name }}. Среди этих параметров только `connection_string` обязателен, а все остальные параметры опциональны и позволяют задать [режим аутентификации](../concepts/auth.md). Если ни один из них не указан, то будет использоваться анонимная аутентификация

```ruby
# Пример указан для плагина ydb_storage, настройка подключения
# в плагинах ydb_topics_output и ydb_topics_input аналогична
ydb_storage {
    # Строка подключения к БД, содержит схему, адрес, порт и путь БД
    connection_string => "grpc://localhost:2136/local"
    # Значение токена аутентификации (режим Access Token)
    token_auth => "...."
    # Путь до файла со значением токена аутентификации (режим Access Token)
    token_file => "/path/to/token/file"
    # Путь до ключа сервисного аккаунта (режим Service Account Key)
    sa_key_file => "/path/to/key.json"
    # Флаг для использования сервиса метаданных (режим Metadata)
    use_metadata => true
}
```
