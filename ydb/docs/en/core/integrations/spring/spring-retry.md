# Automatic transaction retry {{ ydb-short-name }} for Spring

[spring-ydb-retry](https://github.com/ydb-platform/ydb-java-dialects/tree/main/spring-ydb-retry) is a Spring Boot auto-configuration module that adds automatic retry of transactional methods when retriable {{ ydb-short-name }} errors occur.

{% note warning %}

This is an experimental module. Its API and behavior may change in future versions without backward compatibility.

{% endnote %}

{{ ydb-short-name }} is a distributed database, so some errors are transient: for example, a session may expire, a temporary overload may occur, a node may become temporarily unavailable, or a transaction may be aborted due to concurrent data changes. Proper handling of such errors requires retrying the entire transaction, not an individual query. The `spring-ydb-retry` module takes over this logic: it intercepts transactional methods, classifies errors by [{{ ydb-short-name }} status codes](../../reference/ydb-sdk/error_handling.md), and retries the transaction with [exponential backoff](../../concepts/glossary.md#exponential-backoff) and [jitter](../../concepts/glossary.md#jitter) as needed.

## Features {#features}

- Automatic retry of `@Transactional` methods on retriable {{ ydb-short-name }} status codes.
- `@YdbTransactional` annotation with retry settings at the method level (maximum number of attempts, [backoff](../../concepts/glossary.md#exponential-backoff), [idempotence](../../concepts/glossary.md#idempotency)).
- Two-level delay strategy (fast/slow) with [jitter](../../concepts/glossary.md#jitter), tailored to {{ ydb-short-name }} error semantics.
- [Idempotence](../../concepts/glossary.md#idempotency) mode for extended retry coverage on non-deterministic status codes.
- Full configuration via `application.properties` or `application.yaml`.

## Requirements {#requirements}

- Java 17 or newer.
- Spring Boot 3.4+ (based on Spring Framework 6.2+).
- [{{ ydb-short-name }} JDBC Driver](https://github.com/ydb-platform/ydb-jdbc-driver).
- Access to a {{ ydb-short-name }} database instance.

## Installation {#install}

To include the module, two dependencies are required: the {{ ydb-short-name }} JDBC Driver and the `spring-ydb-retry` module itself.

Examples for various build systems:

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

## Usage {#using}

The module is configured automatically via Spring Boot's auto-configuration mechanism. Once the dependency is on the classpath, the module replaces the standard Spring [transaction interceptor](../../concepts/glossary.md#transaction-interceptor) (the `transactionInterceptor` bean) with its own implementation and automatically wraps all methods annotated with `@Transactional` and `@YdbTransactional` with retry logic. No additional annotations or explicit configuration are required — just configure the {{ ydb-short-name }} data source:


```properties
spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
spring.datasource.url=jdbc:ydb:<grpc/grpcs>://<host>:<2135/2136>/path/to/database[?saFile=file:~/sa_key.json]
```


{% note info %}

Retry is applied only to the outer transaction boundary: a method that joins an already open transaction (`PROPAGATION_REQUIRED` when a transaction is active) is not retried separately — the entire transaction is retried at the top level.

{% endnote %}

### @YdbTransactional annotation {#annotation}

`@YdbTransactional` is needed when a specific method requires its own retry settings. For normal retry, `@Transactional` is sufficient.


```java
@Service
public class UserService {

    private final JdbcTemplate jdbcTemplate;

    public UserService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @YdbTransactional(maxAttempts = 5, idempotent = true)
    public void save(String login) {
        jdbcTemplate.update("UPSERT INTO users (login) VALUES (?)", login);
    }
}
```


Retry parameters:

| Attribute | Default | Description |
| --- | --- | --- |
| `enabled` | `true` | Enables or disables retry for the method. Locally, retry can only be disabled — it cannot be enabled on a method if it is disabled globally. |
| `maxAttempts` | `0` | Maximum number of attempts, including the first. For example, `maxAttempts = 5` means up to five method executions total: the first attempt and at most four retries, while `maxAttempts = 1` means exactly one execution with no retries. `0` — take the value from [`ydb.transaction.retry.max-attempts`](#configuration). Negative values are not allowed. |
| `idempotent` | `false` | Marks the method as idempotent. Some {{ ydb-short-name }} status codes are retried only in this mode. |
| `slowBackoffBaseMs` | `0` | Base delay for slow backoff, ms. `0` — from global configuration. |
| `fastBackoffBaseMs` | `0` | Base delay for fast backoff, ms. `0` — from global configuration. |
| `slowCapBackoffMs` | `0` | Ceiling for slow backoff, ms. `0` — from global configuration. |
| `fastCapBackoffMs` | `0` | Ceiling for fast backoff, ms. `0` — from global configuration. |

### Idempotency {#idempotency}

[Transaction retry](../../concepts/glossary.md#transaction-retry) means re-executing the entire transaction. This is safe only for idempotent operations — those whose re-execution yields the same result (for example, `UPSERT` with a deterministic key or read operations).

- Without `idempotent = true`, only status codes that are known to be transient and for which the transaction is guaranteed not to have been applied are retried.
- With `idempotent = true`, non-deterministic status codes (e.g., when the commit result is unknown) are also retried. Enable this mode only for operations whose re-execution is safe.


```java
@YdbTransactional(idempotent = true)
public String findPayload(String guid, int id) {
    return jdbcTemplate.queryForObject(
            "SELECT payload FROM slo_test_table WHERE guid = ? AND id = ?",
            String.class, guid, id);
}
```


### Retry strategy {#policy}

The module extracts the {{ ydb-short-name }} status code from the exception chain and decides whether to retry. First, the code is checked against the retry policy (always retry or only for idempotent operations), then a backoff level is selected for the retriable code:

| Status code | When retried | Backoff level |
| --- | --- | --- |
| `ABORTED` | always | fast |
| `UNAVAILABLE` | always | fast |
| `OVERLOADED` | always | slow |
| `CLIENT_RESOURCE_EXHAUSTED` | always | slow |
| `BAD_SESSION` | always | zero |
| `SESSION_BUSY` | always | zero |
| `UNDETERMINED` | only when `idempotent = true` | fast |
| `TRANSPORT_UNAVAILABLE` (transport error) | only when `idempotent = true` | fast |
| `CLIENT_GRPC_ERROR` | only when `idempotent = true` | fast |
| `SESSION_EXPIRED` | only when `idempotent = true` | zero |

Backoff levels:

- **Fast** — exponential backoff with base `fastBackoffBaseMs` and ceiling `fastCapBackoffMs`.
- **Slow** — exponential backoff with base `slowBackoffBaseMs` and ceiling `slowCapBackoffMs`.
- **Zero** — retry without delay, since it is only related to session recreation.

Other status codes (for example, `TIMEOUT`, `PRECONDITION_FAILED`, `NOT_FOUND`) are not retried.

## Configuration {#configuration}

Global retry behavior is configured in `application.properties` (default values are shown):


```properties
# Enable/disable retry
ydb.transaction.retry.enabled=true

# Maximum number of attempts, including the first
ydb.transaction.retry.max-attempts=10

# Backoff for slow level (OVERLOADED, CLIENT_RESOURCE_EXHAUSTED)
ydb.transaction.retry.slow-backoff-base-ms=50
ydb.transaction.retry.slow-cap-backoff-ms=5000

# Backoff for fast level (ABORTED, UNAVAILABLE, transport errors)
ydb.transaction.retry.fast-backoff-base-ms=5
ydb.transaction.retry.fast-cap-backoff-ms=500
```


| Property | Default | Description |
| --- | --- | --- |
| `ydb.transaction.retry.enabled` | `true` | Global enable/disable retry. |
| `ydb.transaction.retry.max-attempts` | `10` | Maximum number of attempts, including the first. For example, a value of `10` means up to ten method executions total: the first attempt and no more than nine retries. The minimum allowed is `0`. |
| `ydb.transaction.retry.slow-backoff-base-ms` | `50` | Slow backoff base delay, ms. |
| `ydb.transaction.retry.slow-cap-backoff-ms` | `5000` | Slow backoff ceiling, ms. |
| `ydb.transaction.retry.fast-backoff-base-ms` | `5` | Fast backoff base delay, ms. |
| `ydb.transaction.retry.fast-cap-backoff-ms` | `500` | Fast backoff ceiling, ms. |

Parameters `@YdbTransactional` override the global ones for a specific method. A value of `0` in the annotation means 'take from the global configuration'; negative values are not allowed.

## See also {#see-also}

* [{#T}](../../reference/ydb-sdk/error_handling.md)
* [{#T}](../orm/spring-data-jdbc.md)
