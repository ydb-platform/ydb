# Дашборды Grafana для {{ ydb-short-name }}

На этой странице представлено описание дашбордов Grafana для {{ ydb-short-name }}. Как установить дашборды читайте в разделе [{#T}](../../../devops/observability/monitoring.md#prometheus-grafana).

## DB status {#dbstatus}

Общий дашборд базы данных.

Скачать шаблон дашборда **DB status**: [dbstatus.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/dbstatus.json).

## DB overview {#dboverview}

Общий дашборд базы данных по категориям:

- Health
- API
- API details
- CPU
- CPU pools
- Memory
- Storage
- DataShard
- DataShard details
- Latency

Скачать шаблон дашборда **DB overview**: [dboverview.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/dboverview.json).

## Actors {#actors}

Потребление CPU в актор-системе.

| Имя | Описание |
|---|---|
| CPU by execution pool (us) | Потребление CPU в различных пулах исполнения на всех нодах, микросекунды в секунду (один миллион соответствует потреблению одного ядра) |
| Actor count | Количество акторов (по типу актора) |
| CPU | Потребление CPU в различных пулах исполнения (по типу актора) |
| Events | Метрики обработки событий в актор-системе |

Скачать шаблон дашборда **Actors**: [actors.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/actors.json).

## CPU {#cpu}

Потребление CPU в [пулах исполнения](../../../devops/configuration-management/configuration-v2/config-settings.md#tuneconfig).

| Name | Description |
|---|---|
| CPU by execution pool | Потребление CPU в различных пулах исполнения на всех нодах, микросекунды в секунду (один миллион соответствует потреблению одного ядра) |
| Actor count | Количество акторов (по типу актора) |
| CPU | Потребление CPU в различных пулах исполнения |
| Events | Метрики обработки событий в различных пулах исполнения |

Скачать шаблон дашборда **CPU**: [cpu.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/cpu.json).

## gRPC {#grpc}

Метрики слоя gRPC.

| Имя | Описание |
|---|---|
| Requests | Количество запросов, получаемых базой данных в секунду (по типу метода gRPC) |
| Request bytes | Размер запросов, получаемых базой данных, байты в секунду (по типу метода gRPC) |
| Response bytes | Размер ответов, отправляемых базой данных, байты в секунду (по типу метода gRPC) |
| Dropped requests | Количество запросов в секунду, обработка которых была прекращена на транспортном уровне из-за ошибки (по типу метода gRPC) |
| Dropped responses | Количество ответов в секунду, отправка которых была прекращена на транспортном уровне из-за ошибки (по типу метода gRPC) |
| Requests in flight | Количество запросов, которые одновременно обрабатываются базой данных (по типу метода gRPC) |
| Request bytes in flight | Размер запросов, которые одновременно обрабатываются базой данных (по типу метода gRPC) |

Скачать шаблон дашборда **gRPC**: [grpc.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/grpc.json).

## Query engine {#queryengine}

Сведения о движке исполнения запросов.

| Имя | Описание |
|---|---|
| Requests | Количество входящих запросов в секунду (по типу запроса) |
| Request bytes | Размер входящих запросов, байты в секунду (query, parameters, total) |
| Responses | Количество ответов в секунду (по типу ответа) |
| Response bytes | Размеры ответов, байты в секунду (total, query result) |
| Sessions | Сведения об установленных сессиях |
| Latencies | Гистограммы времен исполнения запросов для различных типов запросов |

Скачать шаблон дашборда **Query engine**: [queryengine.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/queryengine.json).

## TxProxy {#txproxy}

Информация от транзакциях с уровня DataShard transaction proxy.

| Имя | Описание |
|---|---|
| Transactions | Метрики транзакций даташардов |
| Latencies | Гистограммы времен исполнения различных этапов транзакций даташардов |

Скачать шаблон дашборда **TxProxy**: [txproxy.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/txproxy.json).

## DataShard {#datashard}

Метрики таблетки DataShard.

| Имя | Описание |
|---|---|
| Operations | Статистика операций с даташардом для разных типов операций |
| Transactions | Информация о транзакциях таблетки даташарда (по типам транзакций) |
| Latencies | Гистограммы времен выполнения различных этапов пользовательских транзакций |
| Tablet latencies | Гистограммы времен выполнения транзакций таблетки |
| Compactions | Сведения о производимых операциях LSM compaction |
| ReadSets | Сведения о пересылаемых ReadSets при исполнении пользовательской транзакции |
| Other | Прочие метрики |

Скачать шаблон дашборда **DataShard**: [datashard.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/datashard.json).

## Database Hive {#database-hive-detailed}

Метрики таблетки [Hive](../../../contributor/hive.md) выбранной базы данных.

На дашборде размещены следующие фильтры:

* database — используется для выбора базы данных, метрики которой необходимо отобразить;
* ds — используется для выбора Prometheus-источника, данные из которого необходимо отобразить на дашборде;
* Tx type — определяет тип транзакции, для которого будут выведены графики на панели "`{Tx type}` average time".

| Имя | Описание |
|---|---|
| CPU usage by HIVE_ACTOR, HIVE_BALANCER_ACTOR | Процессорное время, потребляемое `HIVE_ACTOR` и `HIVE_BALANCER_ACTOR` — двумя самыми важными акторами Hive. |
| Self-ping time | Время ответа таблеткой Hive на собственные запросы. Высокие значения указывают на сильную загрузку (и медленную отзывчивость) Hive. |
| Local transaction times | Время работы CPU, потребляемое для выполнения различных типов локальных транзакций в Hive. Отображает структуру нагрузки на Hive. |
| Tablet count | Общее число таблеток в базе данных. |
| Event queue size | Размер очереди входящих событий. Постоянно высокие значения указывают на то, что Hive не успевает обрабатывать события с требуемой скоростью. |
| `{Tx type}` average time | Среднее время выполнения одной локальной транзакции типа, выбранного в фильтре `Tx type`. |
| Versions | Версии {{ ydb-short-name }}, запущенные на узлах кластера. |
| Hive node | Узел, на котором запущен Hive. |

Скачать шаблон дашборда **Database Hive**: [database-hive-detailed.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/database-hive-detailed.json).
