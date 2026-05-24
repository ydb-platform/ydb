# Enabling metrics in Prometheus

Below are examples of the code for enabling metrics in Prometheus in different {{ ydb-short-name }} SDKs.

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

  This functionality is not currently supported.

- Python

  This functionality is not currently supported.

- JavaScript

  {% include [work-in-progress](../../_includes/work-in-progress.md) %}

- Python

  This functionality is not currently supported.

{% endlist %}
