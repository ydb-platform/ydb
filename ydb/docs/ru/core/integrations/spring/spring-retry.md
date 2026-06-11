# Автоматический повтор транзакций {{ ydb-short-name }} для Spring

[spring-ydb-retry](https://github.com/ydb-platform/ydb-java-dialects/tree/main/spring-ydb-retry) — это модуль автоконфигурации Spring Boot, который добавляет автоматический повтор (retry) транзакционных методов при возникновении повторяемых ошибок {{ ydb-short-name }}.

{% note warning %}

Это экспериментальный модуль. Его API и поведение могут измениться в будущих версиях без сохранения обратной совместимости.

{% endnote %}

{{ ydb-short-name }} является распределённой базой данных, поэтому часть ошибок носит временный (transient) характер: например, переключение лидера таблетки, перегрузка или временная недоступность узла, устаревшая сессия. Корректная обработка таких ошибок требует повторного выполнения транзакции целиком, а не отдельного запроса. Модуль `spring-ydb-retry` берёт эту логику на себя: он перехватывает транзакционные методы, классифицирует ошибки по [кодам статуса {{ ydb-short-name }}](../../reference/ydb-sdk/error_handling.md) и при необходимости повторяет транзакцию с экспоненциальной задержкой и джиттером.

## Возможности {#features}

- Автоматический повтор методов `@Transactional` при повторяемых кодах статуса {{ ydb-short-name }}.
- Аннотация `@YdbTransactional` с настройками повтора на уровне отдельного метода (число попыток, backoff, идемпотентность).
- Двухуровневая стратегия задержки (быстрая/медленная) с джиттером, подобранная под семантику ошибок {{ ydb-short-name }}.
- Режим идемпотентности для расширенного покрытия повторами на недетерминированных кодах статуса.
- Полная конфигурация через `application.properties` или `application.yaml`.

## Требования {#requirements}

- Java 17 или новее.
- Spring Boot 3.4+ (основан на Spring Framework 6.2+).
- [{{ ydb-short-name }} JDBC Driver](https://github.com/ydb-platform/ydb-jdbc-driver).
- Доступ к экземпляру базы данных {{ ydb-short-name }}.

## Установка {#install}

Для подключения модуля потребуются две зависимости: {{ ydb-short-name }} JDBC Driver и сам модуль `spring-ydb-retry`.

Примеры для различных систем сборки:

{% list tabs %}

- Maven

    ```xml
    <!-- Set actual versions -->
    <dependency>
        <groupId>tech.ydb.jdbc</groupId>
        <artifactId>ydb-jdbc-driver</artifactId>
        <version>${ydb.jdbc.version}</version>
    </dependency>

    <dependency>
        <groupId>tech.ydb</groupId>
        <artifactId>spring-ydb-retry</artifactId>
        <version>${spring.ydb.retry.version}</version>
    </dependency>
    ```

- Gradle

    ```groovy
    dependencies {
        // Set actual versions
        implementation "tech.ydb.jdbc:ydb-jdbc-driver:$ydbJdbcVersion"
        implementation "tech.ydb:spring-ydb-retry:$springYdbRetryVersion"
    }
    ```

{% endlist %}

## Использование {#using}

Модуль настраивается автоматически через механизм автоконфигурации Spring Boot. Как только зависимость оказывается в classpath, модуль заменяет стандартный перехватчик транзакций Spring (бин `transactionInterceptor`) на свою реализацию и автоматически оборачивает логикой повтора все методы, помеченные `@Transactional` и `@YdbTransactional`. Никаких дополнительных аннотаций или явного подключения не требуется — достаточно настроить источник данных {{ ydb-short-name }}:

```properties
spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]
```

{% note info %}

Повтор применяется только к внешней границе транзакции: метод, который присоединяется к уже открытой транзакции (`PROPAGATION_REQUIRED` при активной транзакции), не повторяется отдельно — повторяется транзакция целиком на верхнем уровне.

{% endnote %}

### Аннотация @YdbTransactional {#annotation}

`@YdbTransactional` — это расширение стандартной аннотации `@Transactional`, которое добавляет параметры повтора. Её можно использовать как прямую замену `@Transactional`, при этом все стандартные атрибуты (`propagation`, `isolation`, `timeout`, `readOnly`, `transactionManager` и другие) продолжают работать.

```java
@Service
public class UserService {

    private final JdbcTemplate jdbcTemplate;

    public UserService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @YdbTransactional(maxRetries = 5, idempotent = true)
    public void save(String login) {
        jdbcTemplate.update("UPSERT INTO users (login) VALUES (?)", login);
    }
}
```

Атрибуты, относящиеся к повтору:

| Атрибут | Значение по умолчанию | Описание |
| --- | --- | --- |
| `enabled` | `true` | Включает или отключает повтор для аннотированной области. Повтор можно отключить локально, но нельзя включить, если он отключён в глобальной конфигурации. |
| `maxRetries` | `-1` | Максимальное число повторных попыток после первого неудачного выполнения. Метод может быть выполнен суммарно до `maxRetries + 1` раз. Значение `-1` означает наследование из глобальной конфигурации. Значение `0` не поддерживается — для отключения повтора используйте `enabled = false`. Допустимы только `-1` и значения `>= 1`. Глобальный параметр [`ydb.transaction.retry.max-retries`](#configuration) значения `-1` не принимает и должен быть `>= 1`. |
| `idempotent` | `false` | Помечает метод как идемпотентный. Некоторые коды статуса {{ ydb-short-name }} повторяются только для идемпотентных операций. |
| `slowBackoffBaseMs` | `-1` | Базовая задержка медленной стратегии backoff, мс. `-1` — наследование из глобальной конфигурации. |
| `fastBackoffBaseMs` | `-1` | Базовая задержка быстрой стратегии backoff, мс. `-1` — наследование из глобальной конфигурации. |
| `slowCapBackoffMs` | `-1` | Максимальная задержка медленной стратегии backoff, мс. `-1` — наследование из глобальной конфигурации. |
| `fastCapBackoffMs` | `-1` | Максимальная задержка быстрой стратегии backoff, мс. `-1` — наследование из глобальной конфигурации. |

### Идемпотентность {#idempotency}

Повтор транзакции означает её повторное выполнение целиком. Это безопасно только для идемпотентных операций — таких, повторный запуск которых приводит к тому же результату (например, `UPSERT` с детерминированным ключом или операции чтения).

- Без `idempotent = true` повторяются только заведомо временные коды статуса, для которых известно, что транзакция гарантированно не была применена.
- С `idempotent = true` дополнительно повторяются недетерминированные коды статуса (например, когда результат коммита неизвестен). Включайте этот режим только для операций, повторное выполнение которых безопасно.

```java
@YdbTransactional(idempotent = true, readOnly = true)
public String findPayload(String guid, int id) {
    return jdbcTemplate.queryForObject(
            "SELECT payload FROM slo_test_table WHERE guid = ? AND id = ?",
            String.class, guid, id);
}
```

### Стратегия повтора {#policy}

Модуль извлекает код статуса {{ ydb-short-name }} из цепочки исключений и принимает решение о повторе. Сначала код проверяется по политике повтора (повторяется всегда или только для идемпотентных операций), затем для повторяемого кода выбирается уровень backoff:

| Код статуса | Когда повторяется | Уровень backoff |
| --- | --- | --- |
| `ABORTED` | всегда | быстрый |
| `UNAVAILABLE` | всегда | быстрый |
| `OVERLOADED` | всегда | медленный |
| `CLIENT_RESOURCE_EXHAUSTED` | всегда | медленный |
| `BAD_SESSION` | всегда | нулевой |
| `SESSION_BUSY` | всегда | нулевой |
| `UNDETERMINED` | только при `idempotent = true` | быстрый |
| `TRANSPORT_UNAVAILABLE` (транспортная ошибка) | только при `idempotent = true` | быстрый |
| `CLIENT_GRPC_ERROR` | только при `idempotent = true` | быстрый |
| `SESSION_EXPIRED` | только при `idempotent = true` | нулевой |

Уровни backoff:

- **Быстрый** — экспоненциальная задержка с базой `fastBackoffBaseMs` и потолком `fastCapBackoffMs`.
- **Медленный** — экспоненциальная задержка с базой `slowBackoffBaseMs` и потолком `slowCapBackoffMs`.
- **Нулевой** — повтор без задержки, поскольку он связан лишь с пересозданием сессии.

Остальные коды статуса (например, `TIMEOUT`, `PRECONDITION_FAILED`, `NOT_FOUND`) не повторяются.

## Конфигурация {#configuration}

Глобальное поведение повтора настраивается в `application.properties` (приведены значения по умолчанию):

```properties
# Включение/отключение повтора
ydb.transaction.retry.enabled=true

# Максимальное число повторных попыток
ydb.transaction.retry.max-retries=10

# Backoff для медленного уровня (OVERLOADED, CLIENT_RESOURCE_EXHAUSTED)
ydb.transaction.retry.slow-backoff-base-ms=50
ydb.transaction.retry.slow-cap-backoff-ms=5000

# Backoff для быстрого уровня (ABORTED, UNAVAILABLE, транспортные ошибки)
ydb.transaction.retry.fast-backoff-base-ms=5
ydb.transaction.retry.fast-cap-backoff-ms=500
```

| Свойство | Значение по умолчанию | Описание |
| --- | --- | --- |
| `ydb.transaction.retry.enabled` | `true` | Глобальное включение/отключение повтора. |
| `ydb.transaction.retry.max-retries` | `10` | Максимальное число повторных попыток после первого неудачного выполнения. Должно быть `>= 1`; значение меньше `1` (в том числе `0`) приводит к ошибке при старте приложения. Чтобы полностью отключить повтор, используйте `ydb.transaction.retry.enabled=false`. |
| `ydb.transaction.retry.slow-backoff-base-ms` | `50` | Базовая задержка медленного уровня backoff, мс. |
| `ydb.transaction.retry.slow-cap-backoff-ms` | `5000` | Максимальная задержка медленного уровня backoff, мс. |
| `ydb.transaction.retry.fast-backoff-base-ms` | `5` | Базовая задержка быстрого уровня backoff, мс. |
| `ydb.transaction.retry.fast-cap-backoff-ms` | `500` | Максимальная задержка быстрого уровня backoff, мс. |

Значения, заданные в `@YdbTransactional`, переопределяют глобальную конфигурацию для конкретного метода. Атрибуты со значением `-1` наследуют глобальную настройку.

## Смотрите также {#see-also}

* [{#T}](../../reference/ydb-sdk/error_handling.md)
* [{#T}](../orm/spring-data-jdbc.md)
