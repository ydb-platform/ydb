# Включение метрик в Prometheus

<<<<<<< HEAD
Ниже приведены примеры кода включения метрик в Prometheus в разных {{ ydb-short-name }} SDK.

{% list tabs %}

<<<<<<<< HEAD:ydb/docs/ru/core/recipes/ydb-sdk/debug-prometheus.md
- Go (native)
========
- C++

  This feature is not currently supported.

=======
Ниже приведены примеры кода для включения метрик в Prometheus в различных {{ ydb-short-name }} SDK.

{% list tabs %}

>>>>>>> 99a9af9a161 (Auto-translate docs from PR #43637 (#45071))
- Go

  {% list tabs %}

<<<<<<< HEAD
  - Native SDK
>>>>>>>> 99a9af9a161 (Auto-translate docs from PR #43637 (#45071)):ydb/docs/en/core/reference/ydb-sdk/observability/metrics/prometheus.md
=======
  - Нативный SDK
>>>>>>> 99a9af9a161 (Auto-translate docs from PR #43637 (#45071))

    ```go
    package main

    import (
        "context"

        "github.com/prometheus/client_golang/prometheus"
        metrics "github.com/ydb-platform/ydb-go-sdk-prometheus/v2"
        "github.com/ydb-platform/ydb-go-sdk/v3"
        "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    )

    func main() {
        ctx := context.Background()
        registry := prometheus.NewRegistry()
        db, err := ydb.Open(ctx,
            os.Getenv("YDB_CONNECTION_STRING"),
            metrics.WithTraces(
                registry,
                metrics.WithDetails(trace.DetailsAll),
                metrics.WithSeparator("_"),
            ),
        )
        if err != nil {
            panic(err)
        }
        defer db.Close(ctx)
        ...
    }
    ```

<<<<<<< HEAD
- Go (database/sql)
=======
  - database/sql
>>>>>>> 99a9af9a161 (Auto-translate docs from PR #43637 (#45071))

    ```go
    package main

    import (
        "context"
        "database/sql"

        "github.com/prometheus/client_golang/prometheus"
        metrics "github.com/ydb-platform/ydb-go-sdk-prometheus/v2"
        "github.com/ydb-platform/ydb-go-sdk/v3"
        "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    )

    func main() {
        ctx := context.Background()
        registry := prometheus.NewRegistry()
        nativeDriver, err := ydb.Open(ctx,
            os.Getenv("YDB_CONNECTION_STRING"),
            metrics.WithTraces(
                registry,
                metrics.WithDetails(trace.DetailsAll),
                metrics.WithSeparator("_"),
            ),
        )
        if err != nil {
            panic(err)
        }
        defer nativeDriver.Close(ctx)

        connector, err := ydb.Connector(nativeDriver)
        if err != nil {
            panic(err)
        }

<<<<<<< HEAD
        db := sql.OpnDB(connector)
=======
        db := sql.OpenDB(connector)
>>>>>>> 99a9af9a161 (Auto-translate docs from PR #43637 (#45071))
        defer db.Close()
        ...
    }
    ```

<<<<<<< HEAD
          connector, err := ydb.Connector(nativeDriver)
          if err != nil {
              panic(err)
          }

          db := sql.OpenDB(connector)
          defer db.Close()
          ...
      }
      ```

=======
>>>>>>> 99a9af9a161 (Auto-translate docs from PR #43637 (#45071))
  {% endlist %}

- Java

<<<<<<< HEAD
  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Python

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
=======
  Данная функциональность в настоящее время не поддерживается.

- Python

  Данная функциональность в настоящее время не поддерживается.

- JavaScript

  {% include [work-in-progress](../../_includes/work-in-progress.md) %}
>>>>>>> 99a9af9a161 (Auto-translate docs from PR #43637 (#45071))

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

<<<<<<< HEAD
  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#267](https://github.com/ydb-platform/ydb-rs-sdk/issues/267)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
=======
  Отслеживайте прогресс или голосуйте за поддержку Rust SDK: [ydb-rs-sdk#267](https://github.com/ydb-platform/ydb-rs-sdk/issues/267)
>>>>>>> 99a9af9a161 (Auto-translate docs from PR #43637 (#45071))

{% endlist %}
