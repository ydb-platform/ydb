# Using timeouts

The section provides a description of available timeouts and examples of usage in various programming languages.
## Prerequisites for using timeouts {#intro}

The timeout mechanism in {{ ydb-short-name }} is designed to address the following issues:

* Prevent a query from running so long that the query result becomes uninteresting for further use.
* Detect network connectivity issues.

Both of these cases are important for ensuring the overall fault tolerance of the system. Let's take a closer look at timeouts.
## Operation timeout {#operational}

The `operation_timeout` value determines the time during which the query result is of interest to the user. If the operation does not complete within this time, the server returns an error with the `Timeout` code and will attempt to terminate the request, but request cancellation is not guaranteed. Thus, a request for which the user received a `Timeout` error may either be successfully completed on the server or canceled.
## Operation cancellation timeout {#cancel}

The `cancel_after` value determines the time after which the server will start cancelling the request, if cancellation is possible. If the request is successfully cancelled, the server will return an error with the `Cancelled` code.
## Transport timeout {#transport}

For each request, the client must set a transport timeout. This value determines the amount of time the client is willing to wait for a response from the server. If the server does not respond within this time, the client will receive a transport error with the code ``DeadlineExceeded``. It is important to set the client timeout value so that transport timeouts do not trigger during normal operation of the application and network.
{% note tip %}

It is better to set the transport timeout with a margin relative to the response time to the request (measured during load testing or failure testing).

For example, you can use double the value of the 99th percentile of the response time. In other words, the timeout should be equal to $2 × P99$.

The response time for a specific request should be measured in the client code (you should not use the response time metrics provided by the server or data from the query statistics).

{% endnote %}
## Timeout usage {#usage}
### TableService {#usage-tableservice}

It is always recommended to set both an operation timeout and a transport timeout. The transport timeout value should be 50–100 milliseconds more than the operation timeout value to leave some extra time for the client to receive a server error with the ``Timeout`` code.

Example of using timeouts:

{% list tabs group=lang %}

* Python

  ```python
  import ydb

  def execute_in_tx(session, query):
    settings = ydb.BaseRequestSettings()
    settings = settings.with_timeout(0.5)  # transport timeout
    settings = settings.with_operation_timeout(0.4)  # operation timeout
    settings = settings.with_cancel_after(0.4)  # cancel after timeout
    session.transaction().execute(
        query,
        commit_tx=True,
        settings=settings,
    )
  ```
{% if oss == true %}

* C++
  ```cpp
  #include <ydb/public/sdk/cpp/client/ydb.h>
  #include <ydb/public/sdk/cpp/client/ydb_table.h>
  #include <ydb/public/sdk/cpp/client/ydb_value.h>

  using namespace NYdb;
  using namespace NYdb::NTable;

  TAsyncStatus ExecuteInTx(TSession& session, TString query, TParams params) {
    return session.ExecuteDataQuery(
        query
        , TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()
        , TExecDataQuerySettings()
        .OperationTimeout(TDuration::MilliSeconds(300))  // operation timeout
        .ClientTimeout(TDuration::MilliSeconds(400))   // transport timeout
        .CancelAfter(TDuration::MilliSeconds(300)));  // cancel after timeout
  }

  ```
{% endif %}

* Go
  ```go
  import (
    "context"
    "time"

    ydb "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/table"
  )

  func executeInTx(ctx context.Context, s table.Session, query string) {
  ctx, cancel := context.WithTimeout(ctx, time.Millisecond*500) // transport timeout
  defer cancel()
  ctx = ydb.WithOperationTimeout(ctx, time.Millisecond*400)     // operation timeout
  ctx = ydb.WithOperationCancelAfter(ctx, time.Millisecond*400) // cancel after timeout
  tx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())
  _, res, err := s.Execute(ctx, tx, query, table.NewQueryParameters())
  }
  ```
{% endlist %}
### QueryService {#usage-queryservice}

QueryService uses only client timeouts (deadlines). There is no default value, so the query execution time is not limited.

It is recommended to always explicitly set a timeout to avoid waiting for responses to queries that are no longer needed. This also helps to avoid infinite waiting for a server response in unstable network conditions. To detect connection interruptions in a timely manner, it is recommended to additionally configure gRPC KeepAlive parameters.

{% list tabs group=lang %}

* Python

  ```python
  import ydb
  
  settings = (
      ydb.BaseRequestSettings()
      .with_timeout(5)
  )
  
  for result_set in session.execute(query, settings=settings):
      ...
  
  # Or via pool with retry
  result_sets = pool.execute_with_retries(query, settings=settings)
  ```
{% if oss == true %}

* C++
  ```cpp
  #include <ydb-cpp-sdk/client/query/client.h>
  
  auto settings = NYdb::NQuery::TExecuteQuerySettings()
      .Deadline(TDeadline::AfterDuration(std::chrono::seconds(5)));
  
  auto result = session.ExecuteQuery(
      query,
      NYdb::NQuery::TTxControl::NoTx(),
      {},
      settings
  ).GetValueSync();
  ```
{% endif %}

* Go
  ```go
  import (
      "context"
      "time"
      "github.com/ydb-platform/ydb-go-sdk/v3"
  )
  
  ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
  defer cancel()
  
  err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
      res, err := s.Query(ctx, `SELECT 1`)
      return err
  }, query.WithIdempotent())
  ```
* Java
  ```java
  import tech.ydb.query.settings.ExecuteQuerySettings;
  import java.time.Duration;
  
  ExecuteQuerySettings settings = ExecuteQuerySettings.newBuilder()
      .withRequestTimeout(Duration.ofSeconds(5))
      .build();
  
  session.createQuery(query, TxMode.NONE, Params.empty(), settings)
      .execute(part -> {
          ResultSetReader rs = part.getResultSetReader();
          // ...
      }).join();
  
  ```
{% endlist %}
