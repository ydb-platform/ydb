---
title: Using timeouts in YDB
description: 'The operation_timeout value determines the time during which the query result is interesting to the user. If the operation has not been performed during this time, the server returns an error with the Timeout code and tries to terminate the execution of the request, but the cancellation of the request is not guaranteed. It is always recommended to set both the operation timeout and the transport timeout.'
---
# Using timeouts

This section describes available timeouts and provides examples of their usage in various programming languages.

## Prerequisites for using timeouts {#intro}

The timeout mechanism in {{ ydb-short-name }} is designed to:

* Make sure the query execution time doesn't exceed a certain interval after which its result is not interesting for further use.
* Detect network connectivity issues.

Both of these use cases are important for ensuring the fault tolerance of the entire system. Let's take a closer look at timeouts.

## Operation timeout {#operational}

The ``operation_timeout`` value shows the time during which the query result is interesting to the user. If the operation fails during this time, the server returns an error with the ``Timeout`` code and tries to terminate the query, but its cancellation is not guaranteed. So the query that the user was returned the ``Timeout`` error for can be both successfully executed on the server and canceled.

## Timeout for canceling an operation {#cancel}

The ``cancel_after`` value shows the time after which the server will start canceling the query, if it can be canceled. If canceled, the server returns the ``Cancelled`` error code.

## Transport timeout {#transport}

The client must set a transport timeout for each query. This value lets you determine the amount of time that the client is ready to wait for a response from the server. If the server doesn't respond during this time, the client will get a transport error with the ``DeadlineExceeded`` code. Be sure to set such a client timeout value that won't trigger transport timeouts under the normal operation of the application and network.

## Using timeouts {#usage}

We recommend that you always set an operation timeout and transport timeout. The value of the transport timeout should be 50-100 milliseconds more than that of the operation timeout, that way there is some time left for the client to get a server error with the ``Timeout`` code.

Timeout usage example:

{% list tabs %}

- Python

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

- C++

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

- Go

  ```go
  import (
    "context"

    ydb "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/table"
  )

  func executeInTx(ctx context.Context, s table.Session, query string) {
	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*300) // client and by default operation timeout
	defer cancel()
	ctx = ydb.WithOperationTimeout(ctx, time.Millisecond*400)     // operation timeout override
	ctx = ydb.WithOperationCancelAfter(ctx, time.Millisecond*300) // cancel after timeout
	tx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())
	_, res, err := s.Execute(ctx, tx, query, table.NewQueryParameters())
  }
  ```

{% endlist %}

