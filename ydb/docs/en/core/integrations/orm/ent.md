# ent

[ent](https://entgo.io/) is an entity framework for Go that simplifies building and maintaining applications with large data models. Ent provides code generation based on schema definitions, type-safe queries, and automatic migrations.

{{ ydb-short-name }} supports integration with ent through the [ydb-platform/ent](https://github.com/ydb-platform/ent) fork, which adds the `ydb` dialect and YQL query generation support.
This article describes how to connect an Ent-based Go application to {{ ydb-short-name }} using that fork and the compatible [ydb-platform/ariga-atlas](https://github.com/ydb-platform/ariga-atlas) migration tooling.
{% note warning %}

{{ ydb-short-name }} support in Ent is currently in **preview** and requires the [Atlas migration engine](https://entgo.io/docs/migrate#atlas-integration) for schema management.

{% endnote %}

## Installation

1) Install the Ent CLI (for example, with a recent Go toolchain):

    ```bash
    go get entgo.io/ent/cmd/ent@latest
    ```

2) In your `go.mod` file, replace Ent with the fork that supports {{ ydb-short-name }}:

    ```text
    replace entgo.io/ent => github.com/ydb-platform/ent v0.0.1
    ```

3) Replace **Atlas** (the migration engine used by Ent) with the fork that supports {{ ydb-short-name }}:

    ```text
    replace ariga.io/atlas => github.com/ydb-platform/ariga-atlas v0.0.1
    ```

4) Generate a project as described in the [quick introduction](https://entgo.io/docs/getting-started/). For example:

    ```bash
    go run -mod=mod entgo.io/ent/cmd/ent new User
    ```

5) Run `go mod tidy` to refresh `go.mod` and `go.sum`:

    ```bash
    go mod tidy
    ```

## Opening a connection

To connect to {{ ydb-short-name }}, use the standard `ent.Open()` function with the `"ydb"` dialect:

```go
package main

import (
    "context"
    "log"

    "entdemo/ent"
)

func main() {
    client, err := ent.Open("ydb", "grpc://localhost:2136/local")
    if err != nil {
        log.Fatalf("failed opening connection to ydb: %v", err)
    }
    defer client.Close()

    ctx := context.Background()

    if err := client.Schema.Create(ctx); err != nil {
        log.Fatalf("failed creating schema resources: %v", err)
    }
}
```

## Transactions {#transactions}

With the high-level CRUD builders, write operations run inside short-lived transactions via the driver's [`retry.DoTx`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/retry#DoTx), while typical reads use [`retry.Do`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/retry#Do) without an interactive `database/sql` transaction. `Client.BeginTx()` follows the explicit `database/sql` transaction model instead. Ent therefore supports two distinct ways to work with {{ ydb-short-name }}.

### Non-interactive transactions {#non-interactive-transactions}

When using the standard CRUD builders (`.Create()`, `.Query()`, `.Update()`, `.Delete()`), Ent executes operations through ydb-go-sdk's retry helpers:

- **Write operations** (Create, Update, Delete) go through [`retry.DoTx`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/retry#DoTx) — the SDK begins a transaction, executes the operation as a callback, commits, and on a transient error rolls back and re-executes the callback from scratch.
- **Read operations** (Query) go through [`retry.Do`](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/retry#Do) — the SDK obtains a connection, executes the read callback, and retries on transient errors. No explicit transaction is created; the read runs with an implicit snapshot.

This is the recommended way to work with {{ ydb-short-name }} through Ent. Automatic retries and session management are handled transparently.

### Interactive transactions {#interactive-transactions}

When you call `Client.BeginTx()`, Ent opens a transaction via the standard `database/sql` API and returns a `Tx` object. You then perform operations on it and manually call `Commit()` or `Rollback()`. In this mode:

- There is **no callback** for the SDK to re-execute, so automatic retries are not possible.
- Session and transaction lifetime are managed by your code.

Use interactive transactions only when you need explicit control over commit/rollback boundaries that cannot be expressed through the standard builders.

## Automatic retry mechanism {#retry-mechanism}

Because {{ ydb-short-name }} is a distributed database, transient errors (network issues, temporary unavailability, and similar) need explicit handling. The Ent driver for {{ ydb-short-name }} integrates with the [retry package from ydb-go-sdk](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3/retry) to handle these scenarios automatically where possible.

{% note info %}

Automatic retries are not used when creating an interactive transaction via `Client.BeginTx()`. See [{#T}](#interactive-transactions) for details.

{% endnote %}

### Using WithRetryOptions

All CRUD operations support the `WithRetryOptions()` method to configure retry behavior:

```go
import "github.com/ydb-platform/ydb-go-sdk/v3/retry"

// Create
user, err := client.User.Create().
    SetName("John").
    SetAge(30).
    WithRetryOptions(retry.WithIdempotent(true)).
    Save(ctx)

// Query
users, err := client.User.Query().
    Where(user.AgeGT(18)).
    WithRetryOptions(retry.WithIdempotent(true)).
    All(ctx)

// Update
affected, err := client.User.Update().
    Where(user.NameEQ("John")).
    SetAge(31).
    WithRetryOptions(retry.WithIdempotent(true)).
    Save(ctx)

// Delete
affected, err := client.User.Delete().
    Where(user.NameEQ("John")).
    WithRetryOptions(retry.WithIdempotent(true)).
    Exec(ctx)
```

### Retry options

Common retry options from `ydb-go-sdk`:

| Option | Description |
| ------ | ----------- |
| `retry.WithIdempotent(true)` | Mark operation as idempotent, allowing retries on more error types |
| `retry.WithLabel(string)` | Add a label for debugging/tracing |
| `retry.WithTrace(trace.Retry)` | Enable retry tracing |

## Known limitations {#limitations}

### No automatic retries when using Client.BeginTx {#no-retry-begintx}

`Client.BeginTx()` returns a transaction object instead of accepting a callback, so the retry mechanism from ydb-go-sdk cannot be applied. See [{#T}](#interactive-transactions) for details.

If you still need interactive transactions, you may write a retry wrapper manually, as shown in the [ydb-go-sdk examples](https://github.com/ydb-platform/ydb-go-sdk/blob/master/examples/basic/database/sql/series.go).

### No nested transactions {#no-nested-transactions}

{{ ydb-short-name }} uses flat transactions and does not support nested transactions. The Ent driver for {{ ydb-short-name }} returns a no-op (empty) nested transaction object when nested transactions are requested.

### No correlated subqueries {#no-correlated-subqueries}

{{ ydb-short-name }} does not support correlated subqueries with `EXISTS` or `NOT EXISTS`. The Ent integration automatically rewrites such queries to use `IN` with subqueries instead.

### Float/Double index restriction {#float-index-restriction}

{{ ydb-short-name }} does not allow `Float` or `Double` types as index keys. If you define an index on a float field, it will be skipped during migration.

### No native Enum types {#no-native-enums}

{{ ydb-short-name }} does not support enum types in DDL statements. The Ent integration maps enum fields to the `Utf8` (string) type, with validation handled at the application level.

### Primary key requirements {#primary-key-requirements}

{{ ydb-short-name }} requires explicit primary keys for all tables. Define appropriate ID fields in your Ent schemas.

## See also

- [ydb-platform/ent repository on GitHub](https://github.com/ydb-platform/ent)
- [Example project](https://github.com/ydb-platform/ent/tree/ydb-develop/examples/ydb)
- [Ent documentation](https://entgo.io/docs/getting-started)
