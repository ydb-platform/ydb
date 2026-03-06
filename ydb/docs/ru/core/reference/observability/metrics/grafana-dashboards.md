# Дашборды Grafana для {{ ydb-short-name }}

На этой странице представлено описание дашбордов Grafana для {{ ydb-short-name }}.

Как установить и настроить дашборды описано в разделе [по настройке мониторинга кластера YDB](../../../devops/observability/monitoring.md#prometheus-grafana).

## Ydb-essentials {#ydb-essentials}

Общий дашборд.

| Имя | Описание |
| --- | --- |
| `cluster health` | Отображает результаты запросов к `HealthCheck API` |
| `nodes uptime` | Отображает время работы каждого узла с момента его запуска. Помогает отслеживать перезапуски и выявлять нестабильные узлы. |
| `cpu by thread pool (dynnodes)` | Показывает использование `CPU` по пулам потоков динамических узлов. |
| `cpu pool utilization (dynnodes)` | Отображает процент использования выделенных `CPU`-ресурсов динамическими узлами по пулу. Помогает выявлять нехватку `CPU`-ресурсов. |
| `elapsed time vs cpu time` | Показывает время выполнения (`Elapsed us`) vs `CPU`-время (`CPU us`) по каждому узлу. Если соотношение > 100% — `CPU`-время превышает общее время, что, скорее всего, указывает на перераспределение `CPU`. |
| `rss size by node` | Отображает объем оперативной памяти (`Resident Set Size`), используемой каждым узлом. |
| `storage usage` | Показывает общее использование и лимит по размеру всех таблиц базы данных. Помогает предотвратить нехватку пространства для хранения данных. |
| `overloaded shard count` | Показывает количество шардов с высокой загрузкой `CPU` (от 50% до 100%). Помогает оценить уровень перегрузки системы. |
| `queries per second by latency buckets` | Отображает количество запросов в секунду, сгруппированных по интервалам задержек. |
| `transactions per second by latency buckets` | Отображает количество транзакций в секунду, сгруппированных по интервалам задержек. |
| `rows read, uploaded, updated, deleted` | Отображает количество строк в секунду, затронутых соответствующими операциями. |
| `session count by dynnode` | Отображает количество активных сессий по каждому динамическому узлу. Помогает выявлять неравномерность распределения сессий. |
| `query latency percentiles (ms)` | Отображает перцентили времени выполнения запросов. |
| `transaction latency percentiles (ms)` | Отображает перцентили полного времени выполнения транзакций. |
| `yql issues per second (no data = good)` | Показывает количество ошибок, возникших при выполнении `YQL`-запросов. |
| `grpc response errors per second` | Показывает количество ошибок ответов `gRPC` в секунду по типам ошибок. |

Скачать шаблон дашборда **Ydb-essentials**: (Тут будет ссылка).

## Actors {#actors}

Потребление `CPU` в актор-системе.

| Имя | Описание |
| --- | --- |
| `cpu by execution pool (us)` | Потребление `CPU` в различных пулах исполнения на всех нодах, микросекунды в секунду (один миллион соответствует потреблению одного ядра) |
| `actor count` | Количество акторов (по типу актора) |
| `cpu` | Потребление `CPU` в различных пулах исполнения (по типу актора) |
| `events` | Метрики обработки событий в актор-системе |

Скачать шаблон дашборда **Actors**: [actors.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/actors.json).

## CPU {#cpu}

Потребление `CPU` в [пулах исполнения](../../../devops/configuration-management/configuration-v2/config-settings.md#tuneconfig).

| Имя | Описание |
| --- | --- |
| `cpu by execution pool` | Потребление `CPU` в различных пулах исполнения на всех нодах, микросекунды в секунду (один миллион соответствует потреблению одного ядра) |
| `actor count` | Количество акторов (по типу актора) |
| `cpu` | Потребление `CPU` в различных пулах исполнения |
| `events` | Метрики обработки событий в различных пулах исполнения |

Скачать шаблон дашборда **CPU**: [cpu.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/cpu.json).

## gRPC {#grpc}

Метрики слоя `gRPC`.

| Имя | Описание |
| --- | --- |
| `requests` | Количество запросов, получаемых базой данных в секунду (по типу метода `gRPC`) |
| `request bytes` | Размер запросов, получаемых базой данных, байты в секунду (по типу метода `gRPC`) |
| `response bytes` | Размер ответов, отправляемых базой данных, байты в секунду (по типу метода `gRPC`) |
| `dropped requests` | Количество запросов в секунду, обработка которых была прекращена на транспортном уровне из-за ошибки (по типу метода `gRPC`) |
| `dropped responses` | Количество ответов в секунду, отправка которых была прекращена на транспортном уровне из-за ошибки (по типу метода `gRPC`) |
| `requests in flight` | Количество запросов, которые одновременно обрабатываются базой данных (по типу метода `gRPC`) |
| `request bytes in flight` | Размер запросов, которые одновременно обрабатываются базой данных (по типу метода `gRPC`) |

Скачать шаблон дашборда **gRPC**: [grpc.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/grpc.json).

## Query engine {#queryengine}

Сведения о движке исполнения запросов.

| имя | Описание |
| --- | --- |
| `requests` | Количество входящих запросов в секунду (по типу запроса) |
| `request bytes` | Размер входящих запросов, байты в секунду (`query, parameters, total`) |
| `responses` | Количество ответов в секунду (по типу ответа) |
| `response bytes` | Размеры ответов, байты в секунду (`total, query result`) |
| `sessions` | Сведения об установленных сессиях |
| `latencies` | Гистограммы времен исполнения запросов для различных типов запросов |

Скачать шаблон дашборда **Query engine**: [queryengine.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/queryengine.json).

## TxProxy {#txproxy}

Информация о транзакциях с уровня `DataShard transaction proxy`.

| Имя | Описание |
| --- | --- |
| `transactions` | Метрики транзакций даташардов |
| `latencies` | Гистограммы времен исполнения различных этапов транзакций даташардов |

Скачать шаблон дашборда **TxProxy**: [txproxy.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/txproxy.json).

## DataShard {#datashard}

Метрики таблетки `DataShard`.

| Имя | Описание |
| --- | --- |
| `operations` | Статистика операций с даташардом для разных типов операций |
| `transactions` | Информация о транзакциях таблетки даташарда (по типам транзакций) |
| `latencies` | Гистограммы времен выполнения различных этапов пользовательских транзакций |
| `tablet latencies` | Гистограммы времен выполнения транзакций таблетки |
| `compactions` | Сведения о производимых операциях `LSM compaction` |
| `readsets` | Сведения о пересылаемых `ReadSets` при исполнении пользовательской транзакции |
| `other` | Прочие метрики |

Скачать шаблон дашборда **DataShard**: [datashard.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/datashard.json).

## Database Hive {#database-hive-detailed}

Метрики таблетки [Hive](../../../contributor/hive.md) выбранной базы данных.

На дашборде размещены следующие фильтры:

* `database` — используется для выбора базы данных, метрики которой необходимо отобразить;
* `ds` — используется для выбора Prometheus-источника, данные из которого необходимо отобразить на дашборде;
* `Tx type` — определяет тип транзакции, для которого будут выведены графики на панели "`{Tx type}` `average time`".

| Имя | Описание |
| --- | --- |
| `cpu usage by hive_actor, hive_balancer_actor` | Процессорное время, потребляемое `HIVE_ACTOR` и `HIVE_BALANCER_ACTOR` — двумя самыми важными акторами Hive. |
| `self-ping time` | Время ответа таблеткой Hive на собственные запросы. Высокие значения указывают на сильную загрузку (и медленную отзывчивость) Hive. |
| `local transaction times` | Время работы `CPU`, потребляемое для выполнения различных типов локальных транзакций в Hive. Отображает структуру нагрузки на Hive. |
| `tablet count` | Общее число таблеток в базе данных. |
| `event queue size` | Размер очереди входящих событий. Постоянно высокие значения указывают на то, что Hive не успевает обрабатывать события с требуемой скоростью. |
| `{tx type} average time` | Среднее время выполнения одной локальной транзакции типа, выбранного в фильтре `Tx type`. |
| `versions` | Версии {{ ydb-short-name }}, запущенные на узлах кластера. |
| `hive node` | Узел, на котором запущен Hive. |

Скачать шаблон дашборда **Database Hive**: [database-hive-detailed.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/database-hive-detailed.json).

## См. также

- [Справка по метрикам](index.md)
