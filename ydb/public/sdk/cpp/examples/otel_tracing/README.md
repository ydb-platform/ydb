# YDB C++ SDK — OpenTelemetry Demo

Демонстрация трассировки и метрик операций QueryService и TableService
с визуализацией в **Grafana**, **Jaeger** и **Prometheus**.

> ⚠ **Security note.** Демо предназначено только для локального запуска.
> Все порты в `docker-compose.yml` явно привязаны к `127.0.0.1`, чтобы
> стек был доступен только с localhost. Grafana настроена с анонимным
> Admin-доступом и учётными данными `admin/admin` исключительно ради
> удобства one-click входа на дашборды; **не публикуйте этот compose
> на сетевом интерфейсе и не используйте его в продакшене**. Если
> нужно открыть UI с другой машины — поднимайте отдельный экземпляр
> с настроенной аутентификацией и TLS.

## Архитектура

```
┌──────────────┐     OTLP/HTTP      ┌──────────────────┐
│  C++ demo    │ ──────────────────> │  OTel Collector   │
│  application │                    │  :4328 (HTTP)     │
└──────────────┘                    └────────┬──────────┘
                                        │          │
                              traces    │          │  metrics
                                        ▼          ▼
                                  ┌──────────┐  ┌────────────┐
                                  │  Jaeger   │  │ Prometheus  │
                                  │  :16686   │  │ :9090       │
                                  └─────┬─────┘  └──────┬──────┘
                                        │               │
                                        └───────┬───────┘
                                                ▼
                                          ┌──────────┐
                                          │ Grafana   │
                                          │ :3000     │
                                          └──────────┘
```

## Быстрый старт

### 1. Запустить инфраструктуру

```bash
cd examples/otel_tracing
docker compose up -d
```

Дождитесь готовности YDB:

```bash
docker compose logs ydb -f
# Ждите строку "Database started successfully"
```

### 2. Собрать SDK с OTel и тестами

Из корня репозитория:

```bash
mkdir -p build && cd build

cmake .. \
  -DYDB_SDK_TESTS=ON \
  -DYDB_SDK_ENABLE_OTEL_TRACE=ON \
  -DYDB_SDK_ENABLE_OTEL_METRICS=ON

cmake --build . --target otel_tracing_example -j$(nproc)
```

### 3. Запустить демо

```bash
./examples/otel_tracing/otel_tracing_example \
  --endpoint localhost:2136 \
  --database /local \
  --otlp http://localhost:4328 \
  --iterations 20 \
  --retry-workers 6 \
  --retry-ops 30
```

#### Доступные флаги

| Флаг               | По умолчанию              | Описание                                                                 |
|--------------------|---------------------------|--------------------------------------------------------------------------|
| `--endpoint`, `-e` | `localhost:2136`          | gRPC-эндпоинт YDB                                                        |
| `--database`, `-d` | `/local`                  | Имя базы                                                                 |
| `--otlp`           | `http://localhost:4328`   | OTLP/HTTP endpoint коллектора                                            |
| `--iterations`,`-n`| `20`                      | Итераций в Query- и Table-нагрузке                                       |
| `--retry-workers`  | `6`                       | Параллельных воркеров в retry-нагрузке (`0` чтобы пропустить)            |
| `--retry-ops`      | `30`                      | Операций на каждого retry-воркера                                        |
| `--disable-tracing`| off                       | Не передавать `TraceProvider` в YDB SDK                                   |
| `--disable-metrics`| off                       | Не передавать `MetricRegistry` в YDB SDK                                  |
| `--trace-max-queue-size`        | `4096` | `BatchSpanProcessor`: ёмкость in-process очереди спанов               |
| `--trace-schedule-delay-ms`     | `1000` | `BatchSpanProcessor`: интервал между экспортами, мс                   |
| `--trace-max-export-batch-size` | `512`  | `BatchSpanProcessor`: максимум спанов в одном OTLP-вызове             |
| `--metric-export-interval-ms`   | `5000` | `PeriodicExportingMetricReader`: интервал экспорта метрик, мс         |
| `--metric-export-timeout-ms`    | `3000` | `PeriodicExportingMetricReader`: таймаут одного экспорта, мс          |
| `--metric-buffer-flush-ms`      | `100`  | `TMetricBuffer`: интервал flush'а внутреннего буфера, мс (`0` отключает буфер) |
| `--telemetry-drain-sleep-ms`    | `3000` | Пауза после `ForceFlush`, чтобы внешние системы успели принять demo-данные (`0` для бенчмарка) |

#### Демонстрация реальных ретраев

Третий встроенный сценарий — `RunRetryWorkload` — намеренно провоцирует
**SERIALIZABLE-конфликты**: N параллельных воркеров делают
`SELECT → sleep → UPSERT → COMMIT` на одной и той же «горячей» строке
(`id = 9999`) внутри `RetryQuerySync`. YDB возвращает `ABORTED`
проигравшим транзакциям, и SDK прозрачно ретраит их.

В трейсах появятся:

```
RunWithRetry                                                              (INTERNAL, ydb.retry.count=N)
├── Try                                                                   (INTERNAL)  # первая попытка: ydb.retry.attempt и ydb.retry.backoff_ms отсутствуют
│   ├── CreateSession
│   ├── ExecuteQuery
│   └── Commit            db.response.status_code=ABORTED, error.type=ydb_error, exception event
├── Try   ydb.retry.attempt=1                                             (INTERNAL, ydb.retry.backoff_ms=...)
│   └── ...                  db.response.status_code=ABORTED, error.type=ydb_error
└── Try   ydb.retry.attempt=N                                             (INTERNAL, ydb.retry.backoff_ms=...)
    └── ...                  (SUCCESS — атрибут db.response.status_code не выставляется)
```

Для усиления конфликтов поднимите воркеров и операций:

```bash
./examples/otel_tracing/otel_tracing_example \
  --retry-workers 12 --retry-ops 80
```

В конце программа печатает счётчик наблюдённых абортов — каждый из них
соответствует одному автоматическому ретраю SDK.

> **Важно:** для статуса `ABORTED` SDK использует политику
> `RetryImmediately` (см. `src/client/impl/internal/retry/retry.h`),
> поэтому атрибут `ydb.retry.backoff_ms` будет равен `0` —
> это by design. Чтобы увидеть `backoff_ms > 0`, нужны статусы
> `UNAVAILABLE` (FastBackoff, slot 5 ms) или `OVERLOADED` /
> `CLIENT_RESOURCE_EXHAUSTED` (SlowBackoff, slot 1 s). Самый простой способ
> их получить — кратковременно перезапустить YDB во время работы примера:
>
> ```bash
> ./examples/otel_tracing/otel_tracing_example --retry-workers 8 --retry-ops 100 &
> sleep 5
> docker compose -f examples/otel_tracing/docker-compose.yml restart ydb
> wait
> ```

#### Конвейер доставки телеметрии

Демо явно демонстрирует **двойной** уровень батчинга на пути «приложение
→ backend» — это и есть пункт «доставка и пакетная обработка данных»
для телеметрии:

```
   приложение
      │
      │  span                                  metric points
      ▼                                            ▼
  BatchSpanProcessor                  PeriodicExportingMetricReader
  ───────────────────                 ─────────────────────────────
  - in-process очередь                - in-process агрегация
    (max_queue_size)                  - export_interval_ms (push-pull)
  - schedule_delay_ms                 - export_timeout_ms
  - max_export_batch_size                       │
      │  OTLP/HTTP                              │  OTLP/HTTP
      ▼                                         ▼
                  OpenTelemetry Collector
                  ──────────────────────
                  processors: batch
                  (timeout: 1s, send_batch_size: 1024)
                          │
                  ┌───────┴────────┐
                  ▼                ▼
              Jaeger          Prometheus → Grafana
```

* **Spans.** В демо используется `BatchSpanProcessor` (а не
  `SimpleSpanProcessor`): спаны накапливаются в ограниченной очереди и
  отправляются пачками. Параметры доступны через флаги
  `--trace-max-queue-size`, `--trace-schedule-delay-ms`,
  `--trace-max-export-batch-size`.

* **Метрики.** `PeriodicExportingMetricReader` уже реализует pull-batch
  модель: SDK агрегирует точки в памяти и сбрасывает их на коллектор
  каждые `--metric-export-interval-ms`.

* **Коллектор.** `processors: batch` (см. `otel-collector/config.yml`)
  делает второй уровень батчинга поверх входящих OTLP-потоков перед
  пересылкой в Jaeger / Prometheus.

#### Внутренний батчинг эмиссии метрик — `NObservability::TMetricBuffer`

Поверх двух транспортных уровней OTel (`BatchSpanProcessor` и
`PeriodicExportingMetricReader`) в SDK добавлен **третий, внутренний**
уровень пакетной обработки телеметрических данных —
`NObservability::TMetricBuffer`. Он стоит **до** OTel-плагина, на
горячем пути вызовов `IMetricRegistry::Counter()->Inc()` /
`Histogram()->Record()` из `TStatCollector`:

```
   SDK hot-path:                                  ┌────────────────────┐
   TRequestMetrics::End()       Inc/Record   ──▶ │  TMetricBuffer     │
   TStatCollector ...                            │  (этот компонент)  │
                                                 │                    │
                                                 │  thread-local      │
                                                 │  буферы            │
                                                 │  flush раз в       │
                                                 │  MetricBuffer-     │
                                                 │  FlushInterval ms  │
                                                 └─────────┬──────────┘
                                                           │ Add(N),
                                                           │ RecordMany([...])
                                                           ▼
                                                 ┌────────────────────┐
                                                 │  IMetricRegistry   │
                                                 │  (OTel-плагин)     │
                                                 └─────────┬──────────┘
                                                           │
                                                           ▼
                                       PeriodicExportingMetricReader
                                                           │  OTLP
                                                           ▼
                                                   OTel Collector
```

Зачем он нужен сверх того, что уже делает OTel:

* OTel-плагин в C++ выполняет агрегацию **на каждом** `Inc()`/`Record()`:
  hash набора атрибутов → захват мьютекса аггрегатора в OTel SDK →
  обновление бакетов. При высокой нагрузке и общих счётчиках это
  становится узким местом — N потоков сериализуются на одной мьютекс-
  локации в OTel SDK.
* `TMetricBuffer` сдвигает эту работу на отдельный фоновой поток.
  Application-потоки пишут только в свои **thread-local** агрегаты
  (счётчики — `uint64`-инкременты, гистограммы — `small_vector` сэмплов);
  раз в `MetricBufferFlushIntervalMs` фоновый поток обходит все
  буферы и сбрасывает накопленные данные **одним** вызовом
  `ICounter::Add(uint64_t)` / `IHistogram::RecordMany(values)` на
  каждый (instrument, поток).
* Это классический LongAdder / striped counter, недостающий в
  `opentelemetry-cpp`: OTel батчит **экспорт**, `TMetricBuffer`
  батчит **эмиссию**.

Конфигурируется одним флагом — `--metric-buffer-flush-ms` (по умолчанию
`100` ms). `0` отключает буфер и возвращает поведение «прямой записи».

Сам буфер инструментирован собственным набором метрик
(тегируются префиксом `ydb_sdk_metric_buffer_*` — см. раздел Grafana).

### 4. Открыть дашборды

| Сервис     | URL                          | Описание                        |
|-----------|------------------------------|---------------------------------|
| Grafana   | http://localhost:3000         | Дашборд "YDB QueryService"     |
| Jaeger    | http://localhost:16686        | Поиск трейсов по сервису        |
| Prometheus| http://localhost:9090         | Метрики `ydb_client_operation_*`, `ydb_query_session_*` |

**Grafana**: логин `admin` / пароль `admin`.

### 5. Что смотреть

#### В Grafana (дашборд "YDB QueryService"):
- **Operation Rate** — RPS по операциям из `ydb_client_operation_duration_seconds_count`.
- **Failed Operations Rate by status_code** — частота ошибок из
  `ydb_client_operation_failed_total`, разрезано по
  `db.response.status_code`.
- **Operation Duration p50/p95/p99** — распределение из
  `ydb_client_operation_duration_seconds_bucket`.
- **Error Ratio** — `failed / total` по операциям.
- **Query Session Pool** — count по состояниям, pending/timeouts rate,
  configured min/max и create_time-перцентили.
- **Recent Traces** — таблица трейсов из Jaeger.

#### В Jaeger UI:
- Выберите сервис `ydb-cpp-sdk-demo`.
- RPC-спаны (`SpanKind = CLIENT`):
  `CreateSession`, `ExecuteQuery`, `ExecuteDataQuery`,
  `BeginTransaction`, `Commit`, `Rollback`,
  `ExecuteSchemeQuery`, `BulkUpsert`.
- Retry-спаны (`SpanKind = INTERNAL`):
  - `RunWithRetry` — обёртка над всей retryable-логикой.
    При фактических повторах содержит атрибут `ydb.retry.count` (общее число
    выполненных повторов, `>= 1`).
  - `Try` — по одному на каждую попытку. На retry-попытках содержит
    атрибуты `ydb.retry.attempt` (`1..N`) и `ydb.retry.backoff_ms`
    (длительность sleep перед этой попыткой). На первой (не retry) попытке
    эти атрибуты не выставляются.
- Общие атрибуты на всех YDB-спанах:
  - `db.system.name = ydb`
  - `db.namespace` (имя базы)
  - `db.operation.name`
  - `server.address`, `server.port` (эндпоинт балансера)
  - `network.peer.address`, `network.peer.port` (фактический узел кластера)
  - `ydb.node.id`, `ydb.node.dc` — когда удаётся резолвнуть из endpoint pool
- На ошибках:
  - `db.response.status_code` ставится **только** когда
    `error.type == ydb_error`. Для transport / cancellation / прочих исключений
    `status_code` не выставляется.
  - `error.type`:
    - `ydb_error` — серверный YDB-статус;
    - `transport_error` — транспортный/клиентский статус;
    - полное имя типа исключения (`std::system_error`, `TAuthenticationError`, …)
      — для нераспознанных исключений вне retry-классификации.
  - событие `exception` с `exception.type` и `exception.message`.

#### В Prometheus

Operation-уровень:
- `ydb_client_operation_duration_seconds_*` — гистограмма длительности
  (`s`). Лейблы: `db.system.name`, `db.namespace`, `db.operation.name`,
  `server.address`, `server.port`.
- `ydb_client_operation_failed_total` — счётчик неуспешных
  попыток (`{operation}`). Дополнительно к набору лейблов duration-метрики
  присутствует обязательный `db.response.status_code` со значением статуса
  YDB (например `ABORTED`, `TIMEOUT`, `CLIENT_INTERNAL_ERROR`).

Session-pool метрики (для **Query**-клиента — `ydb_query_session_*`,
для **Table**-клиента — `ydb_table_session_*`):
- `*_count` (Gauge, `{session}`) — `state ∈ {idle, used}`.
- `*_create_time_seconds_*` (Histogram, `s`).
- `*_pending_requests_request_total` (Counter, `{request}`) —
  инкрементируется один раз в момент, когда вызывающий встаёт в очередь
  ожидания сессии.
- `*_timeouts_timeout_total` (Counter, `{timeout}`).
- `*_max`, `*_min` (Gauge, `{session}`) — конфигурация пула.

Все pool-метрики имеют тэг `ydb.{query,table}.session.pool.name`. Имя пула
определяется в порядке: явный `TClientSettings::PoolName` → дефолт
`<database>@<endpoint>`.

Метрики `TMetricBuffer` (см. row «SDK Metric Buffer» в дашборде Grafana):
- `ydb_sdk_metric_buffer_pending_updates{instrument=counter|histogram|gauge}` —
  текущее число накопленных, но ещё не сброшенных обновлений по типам
  инструментов; растёт между flush'ами и обнуляется в момент сброса.
- `ydb_sdk_metric_buffer_flush_duration_seconds_*` — гистограмма
  длительности одного flush'а буфера; p50/p95/p99 нарисованы в Grafana.
- `ydb_sdk_metric_buffer_events_buffered_total` — суммарное число
  «логических» обновлений, прошедших через буфер (`Inc()`, `Record()`,
  `Set()`). Делённое на `flushes_total` даёт средний коэффициент
  коалесинга: чем он больше, тем меньше нагрузка на OTel-аггрегатор.
- `ydb_sdk_metric_buffer_flushes_total{trigger=interval|shutdown|threshold}` —
  счётчик сбросов по причине срабатывания.
- `ydb_sdk_metric_buffer_underlying_calls_total{kind=add|record_many|set}` —
  фактическое число вызовов к нижестоящему `IMetricRegistry`; для
  диагностики выигрыша от батчинга.

### 6. Бенчмарк внутреннего батчинга метрик

В `examples/metric_buffer_benchmark` лежит standalone-бенчмарк, который
сравнивает два режима эмиссии в синтетической нагрузке (8 потоков ×
N инкрементов / `Record()` на общий счётчик и общую гистограмму):

1. **`direct`** — без `TMetricBuffer`: каждый `Inc()` / `Record()`
   идёт сразу в `IMetricRegistry` (как сейчас по умолчанию).
2. **`buffered`** — через `TMetricBuffer` с настраиваемым
   `FlushIntervalMs`.

Бенчмарк прогоняется на `TFakeMetricRegistry`, так что измеряется
именно overhead клиентской стороны (без YDB и без OTel-плагина) и
печатает:
- общее время `duration_ms`,
- пропускную способность `ops/sec`,
- число фактических `Inc()`/`Record()` вызовов в registry,
- коэффициент коалесинга (`logical_ops / underlying_calls`).

Запуск:

```bash
cmake --build . --target metric_buffer_benchmark -j$(nproc)
./examples/metric_buffer_benchmark/metric_buffer_benchmark \
    --threads 8 --ops 200000 --flush-ms 100
```

### 7. Остановить

```bash
cd examples/otel_tracing
docker compose down -v
```
