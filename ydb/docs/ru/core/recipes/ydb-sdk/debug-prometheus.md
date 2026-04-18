# Включение метрик в Prometheus

Ниже приведены примеры кода включения метрик в Prometheus в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go

  {% list tabs %}

  - Native SDK

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

  - database/sql

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

          db := sql.OpenDB(connector)
          defer db.Close()
          ...
      }
      ```

  {% endlist %}

- Java

  Функциональность на данный момент не поддерживается.

- Python

  Функциональность на данный момент не поддерживается.

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [work-in-progress](../../_includes/work-in-progress.md) %}

{% endlist %}
