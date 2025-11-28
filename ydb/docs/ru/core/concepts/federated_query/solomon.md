# Чтение данных из Yandex Monitoring (Solomon) через внешние источники данных

В этом разделе описана основная информация о работе с системой мониторинга Yandex Monitoring (Solomon). Solomon позволяет собирать и анализировать метрики инфраструктуры и приложений. Данные в Solomon представляют собой временные ряды, состоящие из значений метрик и описывающих их меток (labels).

## Настройка соединения {#setup-connection}

Чтобы начать читать метрики из Solomon необходимо выполнить следующие шаги:

1.  Создать секрет, содержащий токен для доступа в Solomon.
2.  Создать внешний источник данных, описывающий подключение к конкретной инсталляции Solomon.
3.  Выполнить запрос чтения метрик.


### 1. Создание секрета с токеном

Аутентификация в Solomon выполняется с помощью токена. В случае облачных инсталляций это OAuth-токен, в случае облачных – IAM токен. 

Создайте секрет, содержащий этот токен:

```yql
CREATE OBJECT solomon_token (TYPE SECRET) WITH (value = "<your_token>");
```

### 2. Создание внешнего источника данных

Внешний источник данных описывает параметры подключения к конкретной инсталляции Solomon. Общий синтаксис создания внешнего источника данных для Solomon:

```yql
CREATE EXTERNAL DATA SOURCE <source_name> WITH (
    SOURCE_TYPE="Solomon",

    LOCATION="<solomon_location>",
    [GRPC_LOCATION="<grpc_location>",]

    [PROJECT="<cloud_id>",]
    [CLUSTER="<folder_id>",]

    AUTH_METHOD="TOKEN",
    TOKEN_SECRET_NAME="<secret_name>"
);
```

Где:
* `<source_name>` — произвольное имя создаваемого источника данных.
* `<solomon_location>` — http-эндпоинт конкретной инсталляции Solomon.
* `<grpc_location>` — grpc-эндпоинт конкретной инсталляции Solomon (только для облачных инсталляций).
* `<cloud_id>`, `<folder_id>` — идентификаторы облака и каталога (только для облачных инсталляций).
* `<secret_name>` — имя созданного ранее секрета с токеном.

#### Доступные инсталляции Solomon

| Инсталляция | `LOCATION` | `GRPC_LOCATION` | Обязательные параметры |
| :--- | :--- | :--- | :--- | :--- |
| **Monium Metrics Production** | `solomon.yandex.net` | `solomon.yandex.net` | - |
| **Monium Metrics Prestable** | `solomon-prestable.yandex.net` | `solomon-pre.yandex.net:443` | - |
| **Monium Metrics Testing** | `solomon-test.yandex.net` | `solomon-test.yandex.net:443` | - |
| **Monium Metrics Cloud Prod** | `solomon.cloud.yandex-team.ru` | `monitoring.private-api.cloud.yandex.net:443` | `PROJECT`, `CLUSTER` |
| **Monium Metrics Cloud Preprod** | `solomon.cloud-preprod.yandex-team.ru` | `monitoring.private-api.cloud-preprod.yandex.net:443` | `PROJECT`, `CLUSTER` |
| **Monium Metrics Cloud KZ** | `monitoring.private-api.yacloudkz.tech` | `monitoring.private-api.yacloudkz.tech:443` | `PROJECT`, `CLUSTER` |

**Пример для инсталляции Monium Metrics Production:**
```yql
CREATE EXTERNAL DATA SOURCE solomon_monium_prod WITH (
    SOURCE_TYPE="Solomon",

    LOCATION="solomon.monium.yandex.net",

    AUTH_METHOD="TOKEN",
    TOKEN_SECRET_NAME="solomon_token"
);
```

**Пример для инсталляции Monium Metrics Cloud Prod:**
```yql
CREATE EXTERNAL DATA SOURCE solomon_cloud_prod WITH (
    SOURCE_TYPE="Solomon",

    LOCATION="solomon.cloud.yandex-team.ru",
    GRPC_LOCATION="monitoring.private-api.cloud.yandex.net:443",

    PROJECT="<b1g918jf99v96n47o7u6>", -- Cloud ID
    CLUSTER="b1gaud5b392mmmeolb0k", -- Folder ID

    AUTH_METHOD="TOKEN",
    TOKEN_SECRET_NAME="solomon_cloud_token"
);
```

## Использование внешнего источника данных для чтения метрик

Общий синтаксис запроса к данным Solomon:

```yql
SELECT
    *
FROM
    <solomon_data_source>.<project_or_service>
WITH (
    (selectors|program) = "<query>",
    [labels = "<labels>",]
    from = "<from_time>",
    to = "<to_time>",
    [<downsampling_parameters>]
)
```

Где:
*   `<solomon_data_source>` — идентификатор созданного внешнего источника данных.
*   `<project_or_service>` — проект Solomon (для инсталляций внутри Я) или сервис Solomon (для облака).
*   `<query>` — запрос на языке запросов Solomon.
*   `<labels>` — список имен меток, значения которых нужно получить в отдельных столбцах (опционально).
*   `<from_time>`, `<to_time>` — границы временного интервала в формате [ISO 8601](https://ru.wikipedia.org/wiki/ISO_8601).
*   `<downsampling_parameters>` — параметры прореживания данных (опционально).

{% note tip %}

Запрос к Solomon (содержимое параметров `selectors` или `program`) можно скопировать из веб-интерфейса Solomon. Постройте нужный график в UI, нажмите на три точки справа от запроса, выберите "скорпировать как текст".

{% endnote %}

### Модель данных {#data-model}

Запрос возвращает таблицу, где каждая строка представляет собой точку данных временного ряда.

| Поле | Тип данных | Описание |
| :--- | :--- | :--- |
| `ts` | `Datetime` | Метка времени точки данных. |
| `value` | `Double?` | Значение метрики в точке времени. |
| `type` | `String` | Тип метрики. |
| `labels` | `Dict` | Словарь всех меток метрики. *Присутствует, если не задан параметр `labels`.* |
| `<label_name>` | `String` | Значение конкретной метки. *Присутствует, если она указана в параметре `labels`.* |

### Параметры запроса {#parameters}

| Параметр | Обязательный | Описание | Формат и пример |
| :--- | :--- | :--- | :--- |
| `selectors` | **Да** | Селекторы на языке запросов Solomon | `{cluster="rtmr-sas-test", service="metrics"}` |
| `program` | **Да** | Запрос на языке запросов Solomon | `series_max({method="DescribeTable"})` |
| `labels` | Нет | Список меток для вывода в отдельные колонки. Можно использовать `as` для алиасов. | `"app, cluster, host as server"` |
| `from` | **Да** | Начало искомого временного диапазона. | `"2025-03-12T14:00:00Z"` |
| `to` | **Да** | Конец искомого временного диапазона. | `"2025-03-12T15:00:00Z"` |

### Параметры прореживания (Downsampling) {#downsampling}

| Параметр | Описание | Допустимые значения | По умолчанию |
| :--- | :--- | :--- | :--- |
| `downsampling.disabled` | Отключить прореживание. | `true`, `false` | `false` |
| `downsampling.aggregation` | Функция агрегации для прореживания. | `AVG`, `COUNT`, `LAST`, `MAX`, `MIN`, `SUM` | `AVG` |
| `downsampling.fill` | Стратегия заполнения пропусков. | `NONE`, `NULL`, `PREVIOUS` | `PREVIOUS` |
| `downsampling.grid_interval` | Шаг агрегации в секундах. | Положительное целое число | `15` |

## Примеры {#examples}

### Пример 1: Чтение с использованием параметра selectors (Monium Metrics Production)

```yql
SELECT
    *
FROM
    solomon_prod.nbs
WITH (
    selectors = @@{cluster = "yandexcloud_prod_sas", service = "server_volume", sensor = "Errors/Retriable", request = "*", host = "cluster", type = "-"}@@,

    labels = "app, cluster, host",

    from = "2025-03-12T14:00:00Z",
    to = "2025-03-12T14:00:01Z"
);
```

### Пример 2: Чтение с использованием параметра program (Monium Metrics Cloud Prod)

```yql
SELECT
    *
FROM
    solomon_cloud_prod.ydb
WITH (
    program = @@series_max("api.request.completed_per_second"{})@@,

    from = "2025-03-10T12:00:00Z",
    to = "2025-03-20T12:00:00Z",

    `downsampling.disabled` = "false",
    `downsampling.aggregation` = "AVG",
    `downsampling.grid_interval` = "15"
);
```
