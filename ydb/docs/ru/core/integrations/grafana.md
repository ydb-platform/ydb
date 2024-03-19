# Плагин-источник данных Grafana для интеграции с YDB

Плагин позволяет выводить на графики Grafana данныe, полученные из YDB. Для работы плагина понадобится [Grafana](https://grafana.com/grafana/download?pg=get&plcmt=selfmanaged-box1-cta1) версии не ниже `9.2`, рабочий и доступный экземпляр [YDB](../downloads/index.md).

О том как [установить](https://grafana.com/docs/grafana/latest/getting-started/build-first-dashboard/) Grafana и [плагин](https://grafana.com/docs/grafana/latest/plugins/installation/) можно узнать из [официальной документации](https://grafana.com/docs/grafana/latest/).  

Плагин поддерживает протоколы соединения `gRPCS` и `gRPC`. Если на вашем кластере {{ ydb-short-name }} используются самоподписанные сертификаты TLS, то для соединения с YDB необходимо указать сертификат [Certificate Authority](https://en.wikipedia.org/wiki/Certificate_authority). Подробнее о концепциях соединения с YDB и корневых TLS сертификатах можно узнать из статьи [{#T}](../concepts/connect.md).


## Настройка источника данных { #source-setup }

Новый источник данных YDB можно добавить [вручную](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/) или настроить его с помощью [файла конфигурации](https://grafana.com/docs/grafana/latest/administration/provisioning/#data-sources). Плагин поддерживает ряд различных [типов аутентификации](../reference/ydb-sdk/auth.md) в YDB.

Пример для настройки источника данных с использованием аутентификации через логин/пароль:

```yaml
apiVersion: 1
datasources:
  - name: YDB
    type: ydbtech-ydb-datasource
    jsonData:
      authKind: "UserPassword"
      endpoint: 'grpcs://<hostname>:2135'
      dbLocation: '/location/to/db'
      user: '<username>'
    secureJsonData:
      password: '<userpassword>'
      certificate: |
        <overall content of *.pem file>
```

Поддерживаемые поля для создания соединения:

| Имя  | Описание         |         Тип          |
| :---- | :------------------ | :-------------------: |
| authKind | Тип аутентификации |       `"Anonymous" \| "ServiceAccountKey" \| "AccessToken" \| "UserPassword" \| "MetaData"`        |
| endpoint | Эндпоинт  | `string` |
| dbLocation | Путь к базе  | `string` |
| user | Имя пользователя  | `string` |
| serviceAccAuthAccessKey | Ключ доступа для сервисного аккаунта  | `string` (защищенное поле) |
| accessToken | OAuth-токен  | `string` (защищенное поле) |
| password | Пароль  | `string` (защищенное поле) |
| certificate | Если на вашем кластере {{ ydb-short-name }} используются самоподписанные сертификаты, укажите сертификат [Certificate Authority](https://en.wikipedia.org/wiki/Certificate_authority), через который они были выпущены. | `string` (защищенное поле) |

### Пользователь YDB для источника данных { #ydb-user-setup }

Настройте аккаунт пользователя YDB с возможностью чтения [(подробнее об уровнях доступа)](../cluster/access.md) и доступом до баз данных и таблиц, к которым потребуется делать запросы. Обратите внимание, что Grafana не валидирует запросы с точки зрения их безопасности. Запросы могут содержать в себе любой SQL запросы, включая модифицирующие данные инструкции.

## Написание запросов { #requests }

Для запроса данных из баз YDB поддерживает [YQL диалект](../yql/reference/index.md). Запросы могут содержать макросы, которые упрощают синтаксис и дают возможность динамически изменять параметры, а редактор запросов позволяет получать данные в следующих представлениях: 
* [Временные серии](#time-series).
* [Многострочные временные серии](#multiline-time-series).
* [Таблицы](#tables) и [логи](#visual-logs).

 В запросе могут содержаться два вида макросов - [уровня grafana](#macros) и уровня ydb. Перед отправкой запроса в YDB, плагин проанализирует текст запроса и заменит макросы уровня  grafana на конкретные значения.

### Временные серии { #time-series }

Визуализировать данные как временные серии возможно при условии наличия в результатах запроса одного поля с типами `Date`, `Datetime` или `Timestamp` (на текущий момент поддержана работа со временем только в формате UTC) и как минимум одного поля с типом `Int64`, `Int32`, `Int16`, `Int8`, `Uint64`, `Uint32`, `Uint16`, `Uint8`, `Double` или `Float`. Визуализацию в виде временных серий можно выбрать с помощью настроек. Grafana интерпретирует `timestamp` строки без временной зоны как UTC. Все остальные колонки интерпретируются как значения.

![Time-series](../_assets/grafana/time-series.png)

#### Многострочные временные серии { #multiline-time-series }

Чтобы создать многострочную временную серию, результаты запроса должны содержать в себе как минимум три поля в следующем порядке:
* field 1: `Date`, `Datetime` или `Timestamp` поле
* field 2: значение для группировки
* field 3+: метрики

Например:

```sql
SELECT `timestamp`, `requestTime`, AVG(`responseStatus`) AS `avgRespStatus`
FROM `/database/endpoint/my-logs`
GROUP BY `requestTime`, `timestamp`
ORDER BY `timestamp`
```

### Таблицы { #tables }

Табличное представление доступно для любого валидного YQL запроса ровно с одним набором результатов.

![Table](../_assets/grafana/table.png)

### Визуализация логов { #visual-logs }

Для визуализации данных в виде логов запрос должен возвращать `Date`, `Datetime` или `Timestamp` и `String` значения. Выбрать тип визуализации можно с помощью настроек. По умолчанию только первое встреченное текстовое поле трактуется как строка лога, но это поведение может быть изменено с помощью конструктора запросов.

![Logs](../_assets/grafana/logs.png)

### Макросы { #macroses }

Чтобы упростить синтаксис и получить возможность динамически изменять параметры (например, значение временного диапазона), запрос может содержать в себе макросы. Пример запроса с макросом, который позволяет использовать временной фильтр Grafana.

```sql
SELECT `timeCol`
FROM `/database/endpoint/my-logs`
WHERE $__timeFilter(`timeCol`)
```

```sql
SELECT `timeCol`
FROM `/database/endpoint/my-logs`
WHERE $__timeFilter(`timeCol` + INTERVAL("PT24H"))
```

| Макрос | Описание | Пример вывода |
| ------ | ---------| -------------|
| `$__timeFilter(columnName)`  | Заменяется условием, которое фильтрует данные в указанной колонке или результате выражения на основании временного диапазона, заданного на панели в микросекундах  | `foo >= CAST(1636717526371000 AS TIMESTAMP) AND foo <=  CAST(1668253526371000 AS TIMESTAMP)' )` |
| `$__fromTimestamp` | Заменяется временем начала диапазона, заданного на панели в формате Timestamp | `CAST(1636717526371000 AS TIMESTAMP)` |
| `$__toTimestamp` | Заменяется временем окончания диапазона, заданного на панели в формате Timestamp | `CAST(1636717526371000 AS TIMESTAMP)` |
| `$__varFallback(condition, $templateVar)` | Заменяется первым параметром в том случае, если второй параметр не определен. | `$__varFallback('foo', $bar)` `foo` если переменная `bar` не определена, или значение переменной `bar`  |

### Шаблоны и переменные Templates and variables { #templates-and-variables }

[Инструкция](https://grafana.com/docs/grafana/latest/variables/variable-types/add-query-variable/) по добавлению новой переменной.
После создания переменная может быть использована в запросе к YDB с помощью [специального синтаксиса](https://grafana.com/docs/grafana/latest/variables/syntax/).

```sql
SELECT * FROM `userTable` 
WHERE 
`columnName` == ${customVariable}
```

Для более подробной информации о переменных, см. [документацию Grafana](https://grafana.com/docs/grafana/latest/variables/).

## Дополнительная информация { #extra-info }

* Добавить [Аннотации](https://grafana.com/docs/grafana/latest/dashboards/annotations/).
* Настроить и использовать [Шаблоны и переменные](https://grafana.com/docs/grafana/latest/variables/).
* Добавить [Трансформации](https://grafana.com/docs/grafana/latest/panels/transformations/).
* Настроить [Уведомления](https://grafana.com/docs/grafana/latest/alerting/).
