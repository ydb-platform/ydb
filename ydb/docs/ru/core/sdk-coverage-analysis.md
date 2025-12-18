# Анализ покрытия документации клиентских библиотек YDB

Данный документ содержит анализ статей документации YDB, которые включают примеры кода на различных языках программирования (табы с SDK). Цель — определить объём доработок для обеспечения полного покрытия всех языков.

## Обозначения

- ✅ — язык присутствует в статье
- ❌ — язык отсутствует в статье
- ⚠️ — язык указан, но функциональность не поддерживается

---

## Раздел `recipes/ydb-sdk/` (рецепты кода)

### Основные операции

| Статья | C++ | Go (native) | Go (database/sql) | Java | JDBC | Python | Python (asyncio) | C# (.NET) | PHP | Node.js | Rust |
|--------|:---:|:-----------:|:-----------------:|:----:|:----:|:------:|:----------------:|:---------:|:---:|:-------:|:----:|
| **init.md** — Инициализация драйвера | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| **retry.md** — Повторные попытки | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **upsert.md** — Вставка данных | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **bulk-upsert.md** — Пакетная вставка | ❌ | ✅ | ⚠️ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **tx-control.md** — Режим транзакции | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ |
| **ttl.md** — TTL таблицы | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **vector-search.md** — Векторный поиск | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **session-pool-limit.md** — Пул сессий | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **distributed-lock.md** — Распред. блокировка | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Аутентификация

| Статья | C++ | Go (native) | Go (database/sql) | Java | JDBC | Python | Python (asyncio) | C# (.NET) | PHP | Node.js | Rust |
|--------|:---:|:-----------:|:-----------------:|:----:|:----:|:------:|:----------------:|:---------:|:---:|:-------:|:----:|
| **auth-access-token.md** — Токен | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **auth-anonymous.md** — Анонимная | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **auth-env.md** — Переменные окружения | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ |
| **auth-metadata.md** — Сервис метаданных | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **auth-service-account.md** — Сервисный аккаунт | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **auth-static.md** — Логин и пароль | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |

### Отладка и мониторинг

| Статья | C++ | Go (native) | Go (database/sql) | Java | JDBC | Python | Python (asyncio) | C# (.NET) | PHP | Node.js | Rust |
|--------|:---:|:-----------:|:-----------------:|:----:|:----:|:------:|:----------------:|:---------:|:---:|:-------:|:----:|
| **debug-logs.md** — Логирование | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ |
| **debug-prometheus.md** — Prometheus | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **debug-jaeger.md** — Jaeger | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **debug-otel.md** — OpenTelemetry | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### Балансировка

| Статья | C++ | Go (native) | Go (database/sql) | Java | JDBC | Python | Python (asyncio) | C# (.NET) | PHP | Node.js | Rust |
|--------|:---:|:-----------:|:-----------------:|:----:|:----:|:------:|:----------------:|:---------:|:---:|:-------:|:----:|
| **balancing-random-choice.md** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **balancing-prefer-location.md** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **balancing-prefer-local.md** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **balancing-prefer-pile.md** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## Раздел `reference/ydb-sdk/` (референс SDK)

| Статья | C++ | Go | Java | Python | C# (.NET) | PHP | Node.js | Rust |
|--------|:---:|:--:|:----:|:------:|:---------:|:---:|:-------:|:----:|
| **topic.md** — Работа с топиками | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **coordination.md** — Узлы координации | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **health-check-api.md** — Health Check | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **install.md** — Установка SDK | —* | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **auth.md** — Аутентификация | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |

\* C++ SDK собирается из исходников

---

## Сводка по объёму доработок

### Критические пробелы (высокий приоритет)

| Язык | Отсутствует в статьях | Кол-во |
|------|----------------------|:------:|
| **Rust** | init, retry, upsert, все auth-*, bulk-upsert, все debug-*, все balancing-*, session-pool-limit, distributed-lock, ttl, tx-control, vector-search, topic, coordination, health-check-api | ~25 |
| **Node.js** | retry, upsert, bulk-upsert, все debug-*, все balancing-*, session-pool-limit, distributed-lock, ttl, tx-control, vector-search, topic, coordination, health-check-api | ~18 |
| **C# (.NET)** | retry, upsert, auth-env, bulk-upsert, все debug-*, все balancing-*, session-pool-limit, distributed-lock, ttl, tx-control, vector-search, coordination, health-check-api, auth.md | ~16 |

### Средний приоритет

| Язык | Отсутствует в статьях | Кол-во |
|------|----------------------|:------:|
| **PHP** | retry, upsert, bulk-upsert, debug-prometheus/jaeger/otel, все balancing-*, session-pool-limit, distributed-lock, ttl, vector-search, topic, coordination, health-check-api | ~15 |
| **C++** | init, upsert, все auth-* (кроме static), bulk-upsert, debug-logs, session-pool-limit, distributed-lock, tx-control, auth.md | ~12 |
| **Python / Python (asyncio)** | retry, bulk-upsert, debug-prometheus/jaeger/otel, все balancing-*, session-pool-limit, distributed-lock (Python asyncio также в ttl, vector-search), coordination, health-check-api | ~10 |
| **Java / JDBC** | debug-prometheus/jaeger/otel, все balancing-*, distributed-lock, ttl, tx-control, vector-search, coordination, health-check-api | ~8 |

### Низкий приоритет

| Язык | Отсутствует в статьях | Кол-во |
|------|----------------------|:------:|
| **Go (native / database/sql)** | vector-search, balancing-prefer-pile, health-check-api | ~3 |

---

## Итоговая оценка

| Приоритет | Языки | Оценка трудозатрат |
|-----------|-------|:------------------:|
| Высокий | Rust, Node.js, C# (.NET) | ~60 статей |
| Средний | PHP, C++, Python, Java | ~45 статей |
| Низкий | Go | ~3 статьи |

**Общий объём: ~100+ мест требуют дополнения примерами кода.**

---

## Рекомендации по приоритизации

1. **Rust** — полностью отсутствует почти везде, требует системной доработки
2. **Node.js** — популярный язык, важно для веб-разработчиков
3. **C# (.NET)** — важен для enterprise-сегмента
4. **Python** — один из самых популярных языков, критично закрыть пробелы в retry, debug-*, balancing-*
5. **Java** — важен для enterprise, особенно debug-* и balancing-*

---

*Документ сгенерирован: декабрь 2024*


