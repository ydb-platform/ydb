# Трансфер — поставка access-логов NGINX в таблицу

Эта статья поможет настроить поставку access-логов NGINX в [таблицу](../../concepts/datamodel/table.md) {{ ydb-short-name }} для дальнейшего анализа. В статье рассматривается формат access-логов NGINX, используемый по умолчанию. Более подробно о формате логов NGINX и его настройке можно прочитать в [документации NGINX](https://docs.nginx.com/nginx/admin-guide/monitoring/logging/#set-up-the-access-log).

Формат access-лога NGINX по умолчанию имеет вид:

```txt
$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"
```

Пример:

```txt
::1 - - [01/Sep/2025:15:02:47 +0500] "GET /favicon.ico HTTP/1.1" 404 181 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 YaBrowser/25.6.0.0 Safari/537.36"
::1 - - [01/Sep/2025:15:02:51 +0500] "GET / HTTP/1.1" 200 409 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 YaBrowser/25.6.0.0 Safari/537.36"
::1 - - [01/Sep/2025:15:02:51 +0500] "GET /favicon.ico HTTP/1.1" 404 181 "http://localhost/" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 YaBrowser/25.6.0.0 Safari/537.36"
```

В статье рассматриваются следующие шаги:

* [создание таблицы](#step1), в которую будут записываться данные;
* [создание трансфера](#step2);
* [проверка содержимого таблицы](#step3).

## Пререквизиты

Для выполнения примеров из этой статьи понадобятся:

* Кластер {{ ydb-short-name }} версии 25.2 или выше. Об установке простого одноузлового кластера {{ ydb-short-name }} можно прочитать [здесь](../../quickstart.md). Рекомендации по развёртыванию {{ ydb-short-name }} для промышленного использования см. [здесь](../../devops/deployment-options/index.md).

* Установленный [HTTP-сервер NGINX](https://nginx.org/) с ведением access-логов или доступ к access-логам NGINX с другого сервера.

* Настроенная поставка access-логов NGINX из файла в топик `transfer_recipe/access_log_topic`, например, с помощью [Kafka Connect](../../reference/kafka-api/connect/index.md) с [конфигурацией](../../reference/kafka-api/connect/connect-examples.md#file-to-topic) поставки данных из файла в топик.

## Шаг 1. Создание таблицы {#step1}

Добавьте [таблицу](../../concepts/datamodel/table.md), в которую будут поставляться данные из топика `transfer_recipe/access_log_topic`. Это можно сделать с помощью [SQL-запроса](../../yql/reference/syntax/create_table/index.md):

```yql
CREATE TABLE `transfer_recipe/access_log` (
  partition Uint32 NOT NULL,
  offset Uint64 NOT NULL,
  line Uint64 NOT NULL,
  remote_addr String,
  remote_user String,
  time_local Timestamp,
  request_method String,
  request_path String,
  request_protocol String,
  status Uint32,
  body_bytes_sent Uint64,
  http_referer String,
  http_user_agent Utf8,
  PRIMARY KEY (partition, offset, line)
);
```

Эта таблица `transfer_recipe/access_log` имеет три служебных столбца:

* `partition` — идентификатор [партиции](../../concepts/glossary.md#partition) топика, из которой получено сообщение;
* `offset` — [порядковый номер](../../concepts/glossary.md#offset), идентифицирующий сообщение внутри партиции;
* `line` — порядковый номер строки лога внутри сообщения.

Столбцы `partition`, `offset` и `line` однозначно идентифицируют строку файла access-лога.

Если требуется хранить данные access-логов ограниченное время, можно настроить [автоматическое удаление](../../concepts/ttl.md) старых строк таблицы. Например, для 24 часов это можно сделать с помощью [SQL-запроса](../../yql/reference/recipes/ttl.md):

```yql
ALTER TABLE `transfer_recipe/access_log` SET (TTL = Interval("PT24H") ON time_local);
```

## Шаг 2. Создание трансфера {#step2}

После создания топика и таблицы следует добавить [трансфер](../../concepts/transfer.md) данных, который будет перекладывать сообщения из топика в таблицу. Это можно сделать с помощью [SQL-запроса](../../yql/reference/syntax/create-transfer.md):

```yql
$transformation_lambda = ($msg) -> {
    -- Функция преобразования строки лога в строку таблицы
    $line_lambda = ($line) -> {
        -- Сначала разбиваем строку по символу " (двойные кавычки), чтобы выделить строки, которые могут содержать пробел.
        -- Сами строки символ " (двойные кавычки) содержать не могут — он будет заменён последовательностью символов \x22.
        $parts = String::SplitToList($line.1, '"');
        -- Каждую полученную часть, которая не соответствует экранированной строке, разбиваем по пробелу.
        $info_parts = String::SplitToList($parts[0], " ");
        $request_parts = String::SplitToList($parts[1], " ");
        $response_parts = String::SplitToList($parts[2], " ");
        -- Преобразуем дату в тип Datetime
        $dateParser = DateTime::Parse("%d/%b/%Y:%H:%M:%S");
        $date = $dateParser(SUBSTRING($info_parts[3], 1));

        -- Возвращаем структуру, каждое именованное поле которой соответствует столбцу таблицы.
        -- Важно: типы значений именованных полей должны соответствовать типам столбцов таблицы. Например, если столбец имеет тип Uint32,
        -- значение именованного поля должно быть типа Uint32. В противном случае требуется явное преобразование с помощью CAST.
        -- Значения колонок NOT NULL должны быть извлечены из опционального типа с помощью функции Unwrap.
        return <|
            partition: $msg._partition,
            offset: $msg._offset,
            line: $line.0,
            remote_addr: $info_parts[0],
            remote_user: $info_parts[2],
            time_local: DateTime::MakeTimestamp($date),
            request_method: $request_parts[0],
            request_path: $request_parts[1],
            request_protocol: $request_parts[2],
            status: CAST($response_parts[1] AS Uint32),
            body_bytes_sent: CAST($response_parts[2] AS Uint64),
            http_referer: $parts[3],
            http_user_agent: CAST(String::CgiUnescape($parts[5]) AS Utf8) -- Явно преобразуем в Utf8, так как столбец http_user_agent имеет тип Utf8, а не String
        |>;
    };


    $split = String::SplitToList($msg._data, "\n"); -- Если одно сообщение содержит несколько строк лога, разделяем его на отдельные строки
    $lines = ListFilter($split, ($line) -> { -- Фильтруем пустые строки, которые, например, могут появиться после последнего символа \n 
        return LENGTH($line) > 0;
    });

    -- Преобразуем каждую строку access лога в строку таблицы
    return ListMap(ListEnumerate($lines), $line_lambda);
};

CREATE TRANSFER `transfer_recipe/access_log_transfer`
  FROM `transfer_recipe/access_log_topic` TO `transfer_recipe/access_log`
  USING $transformation_lambda;
```

В этом примере:

* `$transformation_lambda` — это правило преобразования сообщения из топика в колонки таблицы. Каждая строка access лога, записанная в сообщение, обрабатывается отдельно при помощи `line_transformation_lambda`;
* `$line_lambda` — это правило преобразования одной строки access лога в строку таблицы;
* `$msg` — переменная, которая содержит обрабатываемое сообщение из топика.

## Шаг 3. Проверка содержимого таблицы {#step3}

После записи сообщений в топик `transfer_recipe/access_log_topic` спустя некоторое время появятся записи в таблице `transfer_recipe/access_log`. Проверить их наличие можно с помощью [SQL-запроса](../../yql/reference/syntax/select/index.md):

```yql
SELECT *
FROM `transfer_recipe/access_log`;
```

Результат выполнения запроса:

| # | partition | offset | line | remote_addr | remote_user | time_local | request_method | request_path | request_protocol | status | body_bytes_sent | http_referer | http_user_agent |
|---|-----------|--------|------|-------------|-------------|------------|----------------|---------------|------------------|--------|-----------------|--------------|-----------------|
| 1 | 0 | 2 | 0 | ::1 | - | 2025-09-01T15:02:51.000000Z | GET | /favicon.ico | HTTP/1.1 | 404 | 181 | `http://localhost/` | Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 YaBrowser/25.6.0.0 Safari/537.36|
| 2 | 0 | 1 | 0 | ::1 | - | 2025-09-01T15:02:51.000000Z | GET | / | HTTP/1.1 | 200 | 409 | - | Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 YaBrowser/25.6.0.0 Safari/537.36 |
| 3 | 0 | 0 | 0 | ::1 | - | 2025-09-01T15:02:47.000000Z | GET | /favicon.ico | HTTP/1.1 | 404 | 181 | - | Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 YaBrowser/25.6.0.0 Safari/537.36 |

{% include [x](_includes/batching.md) %}

## Заключение

В этой статье показан пример доставки access-логов NGINX в таблицу {{ ydb-short-name }}. Логи любого другого текстового формата можно обрабатывать аналогичным образом: нужно создать таблицу для хранения необходимых данных из лога и правильно написать [lambda-функцию](../../yql/reference/syntax/expressions.md#lambda), преобразующую строки лога в строки таблицы.

## Смотрите также

* [{#T}](../../concepts/transfer.md)
* [{#T}](quickstart.md)
* [Lambda-функция](../../yql/reference/syntax/expressions.md#lambda)
* [{#T}](../../yql/reference/syntax/create_table/index.md)
* [{#T}](../../yql/reference/syntax/create-topic.md)
* [{#T}](../../yql/reference/syntax/create-transfer.md)
* [Функция UNWRAP](../../yql/reference/builtins/basic.md#unwrap)
* [Функция COALESCE](../../yql/reference/builtins/basic.md#coalesce)
