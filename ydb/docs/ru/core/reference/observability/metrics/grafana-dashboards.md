# Дашборды Grafana для {{ ydb-short-name }}

На этой странице представлено описание дашбордов Grafana для {{ ydb-short-name }}.

Как установить и настроить дашборды описано в разделе [по настройке мониторинга кластера YDB](../../../devops/observability/monitoring.md#prometheus-grafana).

## YDB Essential Metrics {#ydbessentials}

Дашборд для мониторинга ключевых показателей базы данных.

### Секция Health {#ydbessentials-health}

В этой секции размещены графики, отображающие состояние компонентов кластера и базы данных.

| Имя | Описание |
| --- | --- |
| `Nodes count` | Количество запущенных узлов YDB, в шт. |
| `Nodes Uptime` | Время непрерывной работы каждого узла с момента запуска; помогает обнаруживать перезапуски и нестабильные узлы, в секундах. |
| `VDisks count` | Количество доступных VDisk в кластере, в шт. |

### Секция Saturation {#ydbessentials-saturation}

В этой секции размещены графики, отражающие утилизацию ресурсов базы данных.

| Имя | Описание |
| --- | --- |
| `CPU by thread pool (dynnodes)` | Потребление CPU динамическими узлами в разрезе [пулов исполнения](../../../devops/configuration-management/configuration-v2/config-settings.md#tuneconfig), в ядрах CPU. |
| `CPU utilization (dynnodes)` | Утилизация CPU динамическими узлами в разрезе [пулов исполнения](../../../devops/configuration-management/configuration-v2/config-settings.md#tuneconfig), в %. |
| `Elapsed Time vs CPU Time` | Соотношение реального времени выполнения операций (`ElapsedMicrosec`) и процессорного времени (`CpuMicrosec`) по узлам. Устойчивое превышение отметки 100% означает, что сессии проводят время в ожидании, а не в активной работе: как правило, это ожидание I/O или CPU overcommit на стороне гипервизора. |
| `RSS size by node` | Объём оперативной памяти (Resident set size), потребляемой каждым динамическим узлом, с отображением лимитов памяти cgroup, в байтах. |
| `Storage usage` | Логический размер базы данных и установленный на него лимит, в байтах. |
| `Overloaded shard count` | Количество [перегруженных DataShard](../../../troubleshooting/performance/schemas/overloaded-shards.md) по диапазонам загрузки CPU — от 50% до 100%, в шт. |

### Секция Traffic {#ydbessentials-traffic}

В этой секции расположены графики, характеризующие нагрузку на базу данных.

| Имя | Описание |
| --- | --- |
| `Queries per second by latency buckets` | Количество запросов в секунду с разбивкой по диапазонам задержки (от 1 мс до +∞). Каждый диапазон выделен отдельным цветом — от зелёного для быстрых запросов до фиолетового для медленных. Позволяет оценить распределение задержек и общий rps, в шт/с |
| `Transactions per second by latency buckets` | Количество транзакций в секунду с разбивкой по диапазонам задержки (от 1 мс до +∞). Каждый диапазон выделен отдельным цветом — от зелёного для быстрых транзакций до фиолетового для медленных. Позволяет оценить распределение задержек и общий tps, в шт/с |
| `Rows read, uploaded, updated, deleted` | Количество операций со строками таблиц в секунду: чтение, создание, обновление и удаление, в операциях/с |
| `Session count by dynnode` | Количество активных сессий на каждом динамическом узле, в шт |

### Секция Latency {#ydbessentials-latency}

В этой секции расположены графики, отображающие время выполнения запросов и транзакций.

| Имя | Описание |
| --- | --- |
| `Query latency percentiles (ms)` | Время выполнения запросов к базе данных в перцентилях p50, p90, p95, p99, в миллисекундах |
| `Transaction latency percentiles (ms)` | Время выполнения транзакций в базе данных в перцентилях p50, p90, p95, p99, в миллисекундах |

### Секция Errors {#ydbessentials-errors}

В этой секции расположены графики, отображающие количество возникающих ошибок.

| Имя | Описание |
| --- | --- |
| `YQL Issues per second` | Количество ошибок выполнения YQL-запросов по типам ошибок, в шт/с. |
| `GRPC response errors per second` | Количество gRPC-ответов с ошибками с разбивкой по статусам, в шт/с. |

Скачать шаблон дашборда **YDB Essential Metrics**: [ydb-essentials.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/ydb-essentials.json).

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

| Имя | Описание |
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

## Topic {#topic}

На дашборде отображаются графики по метрикам одного топика. Название топика задаётся в фильтре `topic` в верхней части дашборда. Ниже перечислены панели и расшифровка метрик.

| Имя | Описание |
| --- | --- |
| `Total incoming records (bytes) per second` | Количество байт в секунду, записанных в топик методом `Ydb::TopicService::StreamWrite` |
| `Total incoming records (count) per second` | Количество сообщений в секунду, записанных методом `Ydb::TopicService::StreamWrite` |
| `Write latency` | Длительность записи: время от создания сообщения до его записи в топик. Доля сообщений (в процентах), для которых длительность записи уложилась в интервалы <100 мс, <200 мс и т.д. |
| `Partition throttling` | Длительность троттлинга записи - ожидания доступной квоты на запись. Процент сообщений, длительность троттлинга записи которых уложилась в интервалы <1 мс, <5 мс и т.д. |
| `Partition quota usage` | Утилизация квот партиций топика на запись, % |
| `Write sessions active` | Количество открытых сессий записи в топик |
| `Write sessions created` | Количество создаваемых в секунду сессий записи в топик |

Скачать шаблон дашборда **Topic**: [topic.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/topic.json).

## Topic — Consumer {#topic-consumer}

На дашборде отображаются графики по метрикам одного топика и связанного с ним читателя. Топик выбирается в фильтре `topic`, читатель — в фильтре `consumer`. Ниже перечислены панели и расшифровка метрик.

| Имя | Описание |
| --- | --- |
| `Total incoming records (bytes) per second` | Количество байт в секунду, записанных в топик методом `Ydb::TopicService::StreamWrite` |
| `Total outgoing records (bytes) per second` | Количество байт в секунду, прочитанных из топика читателем методом `Ydb::TopicService::StreamRead` |
| `Total incoming records (count) per second` | Количество сообщений в секунду, записанных в топик методом `Ydb::TopicService::StreamWrite` |
| `Total outgoing records (count) per second` | Количество сообщений в секунду, прочитанных из топика читателем методом `Ydb::TopicService::StreamRead` |
| `End-to-end latency` | End-to-end длительность: время от момента создания сообщения до момента его чтения. Процент сообщений, end-to-end длительность для которых уложилась в интервалы <100 мс, <200 мс и т.д. |
| `Read latency max` | Максимальная (по всем партициям) разница между текущим временем и временем записи последнего сообщения в топик, мс |
| `Unread messages max` | Максимальная (по всем партициям) разница между последним смещением в партиции и последним прочитанным смещением, в сообщениях |
| `Read idle time max` | Максимальное время простоя (сколько времени консьюмер не читал из партиции) по всем партициям топика, мс |
| `Uncommitted messages max` | Максимальная (по всем партициям) разница между последним смещением в партиции и последним закомиченным смещением, в сообщениях |
| `Committed read lag max` | Максимальная (по всем партициям) разница между текущим временем и временем записи последнего закомиченного сообщения в топик, мс |
| `Partition sessions started` | Количество сессий чтения топика читателем, запущенных в секунду |

Скачать шаблон дашборда **Topic — Consumer**: [topic-consumer.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/topic-consumer.json).
