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

#### Демонстрация реальных ретраев

Третий встроенный сценарий — `RunRetryWorkload` — намеренно провоцирует
**SERIALIZABLE-конфликты**: N параллельных воркеров делают
`SELECT → sleep → UPSERT → COMMIT` на одной и той же «горячей» строке
(`id = 9999`) внутри `RetryQuerySync`. YDB возвращает `ABORTED`
проигравшим транзакциям, и SDK прозрачно ретраит их.

В трейсах появятся:

```
ydb.RunWithRetry                                                              (INTERNAL, ydb.retry.count=N)
├── ydb.Try                                                                   (INTERNAL)              # первая попытка: ydb.retry.attempt и ydb.retry.backoff_ms отсутствуют
│   ├── ydb.CreateSession
│   ├── ydb.ExecuteQuery
│   └── ydb.Commit            db.response.status_code=ABORTED, error.type=ydb_error, exception event
├── ydb.Try   ydb.retry.attempt=1                                             (INTERNAL, ydb.retry.backoff_ms=...)
│   └── ...                  db.response.status_code=ABORTED, error.type=ydb_error
└── ydb.Try   ydb.retry.attempt=N                                             (INTERNAL, ydb.retry.backoff_ms=...)
    └── ...                  db.response.status_code=SUCCESS
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

### 4. Открыть дашборды

| Сервис     | URL                          | Описание                        |
|-----------|------------------------------|---------------------------------|
| Grafana   | http://localhost:3000         | Дашборд "YDB QueryService"     |
| Jaeger    | http://localhost:16686        | Поиск трейсов по сервису        |
| Prometheus| http://localhost:9090         | Метрики `db_client_operation_*` |

**Grafana**: логин `admin` / пароль `admin`.

### 5. Что смотреть

#### В Grafana (дашборд "YDB QueryService"):
- **Request Rate by Operation** — RPS по операциям (ExecuteQuery, ExecuteDataQuery, CreateSession, Commit, Rollback)
- **Error Rate by Operation** — частота ошибок
- **Duration p50/p95/p99** — распределение длительности операций
- **Error Ratio** — процент ошибок
- **Recent Traces** — таблица трейсов из Jaeger

#### В Jaeger UI:
- Выберите сервис `ydb-cpp-sdk-demo`.
- RPC-спаны (`SpanKind = CLIENT`):
  `ydb.CreateSession`, `ydb.ExecuteQuery`, `ydb.ExecuteDataQuery`,
  `ydb.BeginTransaction`, `ydb.Commit`, `ydb.Rollback`,
  `ydb.ExecuteSchemeQuery`, `ydb.BulkUpsert`.
- Retry-спаны (`SpanKind = INTERNAL`):
  - `ydb.RunWithRetry` — обёртка над всей retryable-логикой.
    При фактических повторах содержит атрибут `ydb.retry.count` (общее число
    выполненных повторов, `>= 1`).
  - `ydb.Try` — по одному на каждую попытку. На retry-попытках содержит
    атрибуты `ydb.retry.attempt` (`1..N`) и `ydb.retry.backoff_ms`
    (длительность sleep перед этой попыткой). На первой (не retry) попытке
    эти атрибуты не выставляются.
- Общие атрибуты на всех YDB-спанах:
  - `db.system.name = ydb`
  - `db.namespace` (имя базы)
  - `server.address`, `server.port` (эндпоинт балансера)
  - `network.peer.address`, `network.peer.port` (фактический узел кластера)
- На ошибках добавляются:
  - `db.response.status_code` — строковый статус YDB (например, `ABORTED`)
  - `error.type` — категория источника ошибки: `ydb_error` (ошибка,
    возвращённая YDB) или `transport_error` (ошибка транспортного уровня)
  - событие `exception` с `exception.type` и `exception.message`

#### В Prometheus:
- `db_client_operation_duration_seconds_bucket` — гистограмма длительности
  (OTel Semantic Conventions). Лейблы: `db.system.name`, `db.namespace`,
  `db.operation.name` (с префиксом `ydb.`), `ydb.client.api`
  (`Query` / `Table`). Для ошибок добавляются `db.response.status_code`
  (точный YDB-статус, например `ABORTED`) и `error.type` —
  низкокардинальная категория источника ошибки: `ydb_error` (статусы YDB-сервера)
  или `transport_error` (клиентские/транспортные статусы).
- `db_client_operation_requests_total` — счётчик начатых операций
  (включая каждую попытку ретрая).
- `db_client_operation_errors_total` — счётчик неуспешных попыток.
  Полезно сравнивать с `requests_total`: для retry-нагрузки на той же
  «горячей» строке коэффициент ошибок будет очень высоким — это и есть
  индикатор работы ретраев.

### 6. Остановить

```bash
cd examples/otel_tracing
docker compose down -v
```
