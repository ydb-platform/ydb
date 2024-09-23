---
title: "Instructions for setting the session pool size in {{ ydb-short-name }}"
description: "The article will tell you how to set the session pool size in different {{ ydb-short-name }} SDKs."
---

# Setting the session pool size

{% include [work in progress message](_includes/addition.md) %}

The client's session pool size affects resource consumption (RAM, CPU) on the server side of {{ ydb-short-name }}. Simple math: if `1000` clients of the same DB have `1000` sessions each, `1000000` actors (workers, session performers) are created on the server side. If you don't limit the number of sessions on the client, this may result in a slow cluster that is close to a failure.

By default, the {{ ydb-short-name }} SDK has a limit of `50` sessions when using native drivers. There is no limit for third-party libraries, such as Go database/sql.

A good recommendation is to set the limit on the number of client sessions to the minimum required for the normal operation of the client app. Keep in mind that sessions are single-threaded both on the server and client side. So if the application needs to make `1000` simultaneous (`inflight`) requests to {{ ydb-short-name }} for its estimated load, the limit should be set to `1000` sessions.

Here it's necessary to distinguish between the estimated `RPS` (requests per second) and `inflight`. In the first case, this is the total number of requests to {{ ydb-short-name }} completed within `1` second. For example, if `RPS`=`10000` and the average `latency` is `100`ms, it's sufficient to set the session limit to `1000`. This means that each session will perform an average of `10` consecutive requests for the estimated second.

Below are examples of the code for setting the session pool limit in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

  ```golang
  package main

  import (
    "context"

    "github.com/ydb-platform/ydb-go-sdk/v3"
  )

  func main() {
    db, err := ydb.Open(ctx,
      os.Getenv("YDB_CONNECTION_STRING"),
      ydb.WithSessionPoolSizeLimit(500),
    )
    if err != nil {
      panic(err)
    }
    defer db.Close(ctx)
    ...
  }
  ```

- Go (database/sql)

  The `database/sql` library has its own connection pool. Each `database/sql`connection corresponds to a specific {{ ydb-short-name }} session. A `database/sql` connection pool is managed by the `sql.DB.SetMaxOpenConns` and `sql.DB.SetMaxIdleConns` functions. Learn more in the `database/sql` [documentation](https://pkg.go.dev/database/sql#DB.SetMaxOpenConns).

  Example of the code that uses the size of `database/sql` connection pool:
  ```golang
  package main

  import (
    "context"
    "database/sql"

    _ "github.com/ydb-platform/ydb-go-sdk/v3"
  )

  func main() {
    db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
    if err != nil {
      panic(err)
    }
    defer db.Close()
    db.SetMaxOpenConns(100)
    db.SetMaxIdleConns(100)
    db.SetConnMaxIdleTime(time.Second) // workaround for background keep-aliving of YDB sessions
    ...
  }
  ```

- Java

  ```java
  this.tableClient = TableClient.newClient(transport)
          // 10 - minimum number of active sessions to keep in the pool during the cleanup
          // 500 - maximum number of sessions in the pool
          .sessionPoolSize(10, 500)
          .build();
  ```

{% endlist %}
