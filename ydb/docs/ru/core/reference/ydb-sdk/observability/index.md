# Наблюдаемость в {{ ydb-short-name }} SDK

В этом разделе описано подключение средств диагностики {{ ydb-short-name }} SDK — логирования, метрик и распределённой трассировки. Рекомендуется подключать их заранее, до возникновения проблем, чтобы при расследовании инцидента видеть полную картину состояния системы до, во время и после сбоя.

Логирование:

- [{#T}](logging/logging.md)
- [{#T}](logging/opentelemetry.md)

Метрики:

- [{#T}](metrics/opentelemetry.md)
- [{#T}](metrics/prometheus.md)

Трассировка:

- [{#T}](tracing/opentelemetry.md)
- [{#T}](tracing/jaeger.md)

Серверная наблюдаемость {{ ydb-short-name }}, не зависящая от SDK, описана в разделе [{#T}](../../observability/index.md).
