# Настройка интерграции между Logstash и {{ ydb-short-name }}

В разделе представлены варианты интеграции между системой сбора и анализа данных Logstash и {{ ydb-short-name }}.

## Введение

[Logstash](https://www.elastic.co/logstash) – инструмент сбора, фильтрации и нормализации логов. Данный продукт позволяет динамически получать, фильтровать
и доставлять данные независимо от их формата и сложности. Конфиграция системы производиться с помощью плагинов различных типов - input, output и filter.

Для работы с Logstash мы подготовили [набор плагинов](https://github.com/ydb-platform/ydb-logstash-plugins)

* Storage Plugin для сохранения данных в таблице ([строчной](../concepts/datamodel/table.md#strokovye-tablicy) или [колоночной](../concepts/datamodel/table.md#column-tables)) {{ ydb-short-name }} 

* Input Topic Plugin для чтение данных из [топика](../concepts/topic.md) {{ ydb-short-name }}

* Output Topic Pluing для отправки данных в [топик](../concepts/topic.md) {{ ydb-short-name }}

Плагины можно [собрать](https://github.com/ydb-platform/ydb-logstash-plugins/blob/main/BUILD.md) самостоятельно из исходного кода либо воспользовать готовыми [сборками](https://github.com/ydb-platform/ydb-logstash-plugins/releases) под две последнии версии Logstash

Установить любой собранный плагин в Logstash можно с помощью команды
```bash
path-to-logstash/bin/logstash-plugin install /path/to/logstash-plugin.gem
```

Проверить что плагин установлен можно командой
```bash
path-to-logstash/bin/logstash-plugin list
```
Которая выведет список всех установленных плагинов

## Использование плагина Storage

Данный плагин позволяет перенаправить поток данных из Logstash в таблицу {{ ydb-short-name }} для дальнешего анализа. Для этого удобно использовать [колоночные таблицы](../concepts/datamodel/table.md#column-tables), оптимизированные для OLAP запросов.

### Создание колоночной таблицы для хранения записей
В выбранной базе данных {{ ydb-short-name }} мы можем создать новуб колоночную таблицу с нужными нами колонками. Ограничения накладываются на количество и тип колонок главного ключа - это обязательно должна быть одна колонка с типом Text, так как именно такой идентификатор используется в Logstash.

```sql
CREATE TABLE `logstash_demo` (
    `rec_id`   Text NOT NULL,  -- Идентификатор обязательно должен иметь тип Text
    `message`  Text,           -- Все остальные колонки могут иметь любой поддерживаемый {{ ydb-short-name }} тип
    `user`     Text,           
    `level`    Int32,

    PRIMARY KEY (
         `rec_id` -- Главный ключ всегда должен состоять из одной колонки идентификатора
    )
)
WITH (STORE = COLUMN);
```

### Конфигурация плагина

Для настройки плагина мы должен добавить секцию `ydb_storage_plugin` в раздел `output` файла конфигурации [Logstash](https://www.elastic.co/guide/en/logstash/current/configuration.html). Здесь указаны только специфичные для данного плагина опции, об опция подключения к {{ ydb-short-name }} можно прочитать в разделе [Конфигурация подключения плагинов к  {{ ydb-short-name }}](#konfiguraciya-podklyucheniya-plaginov-k-ydb)

```ruby
...
output {
  ...
  ydb_storage_plugin {
    ...                                                  # Опции подключения к {{ ydb-short-name }}
    table_name => "logstash_demo"                        # Имя таблицы (по умолчанию logstash)
    name_identifier_column => "rec_id"                   # Имя колонки идентификатора (по умолчанию id)
    columns => "message, Text, user, Text, level, Int32" # Список колонок и их типов.
  }
}
```
После изменения данного файла logstash нужно перезапустить.

### Отправка тестового сообщения
Для проверки работоспособности плагина можно добавить возможность отправки сообщений в Logstash через http (добавив плагин `http` в секцию `input`)
```ruby
...
input {
  ...
  http {
    port => 9876 # Можно указать любой порт
  }
}
```
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

## Использование плагина Output Topic

## Конфигурация подключения плагинов к {{ ydb-short-name }}

Все плагины используют одни и те же параметры для настройки подключения к базе данных {{ ydb-short-name }}. Среди этих параметров только `connection_string` обязателен, все остальные параметры опциональны и задают режим аутентификации. Если ни один из них не указан - то будет использоваться анонимная аутентфикация

```ruby
<ydb_plugin> {
    # Строка подключения к БД, содержит схему, endpoint, порт и путь БД
    connection_string => "grpc://localhost:2136/local"
    # Значение токена аутентифкации - нужно указать если используется аутентифкация по Access Token
    token_auth => "...."
    # Путь до ключа сервисного аккаунта - нужно указать если используется аутентфикация по сервисному аккаунту
    sa_key_file => "/path/to/key.json"
}
```
