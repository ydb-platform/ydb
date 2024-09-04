# Выполнение повторных попыток

{% include [work in progress message](_includes/addition.md) %}

{{ ydb-short-name }} является распределенной СУБД с автоматическим масштабированием под нагрузку.
На серверной стороне могут проводиться работы, серверные стойки или целые дата-центры могут быть временно отключены.
В связи с этим допускаются некоторые ошибки при работе с {{ ydb-short-name }}.
В зависимости от типа ошибки следует по разному реагировать на них.
{{ ydb-short-name }} SDK для обеспечения высокой доступности предоставляют встроенные средства выполнения повторных попыток,
в которых учтены типы ошибок и закреплена реакция на них.

Ниже приведены примеры кода использования встроенных в {{ ydb-short-name }} SDK средств выполнения повторных попыток:

{% list tabs %}

- Go (native)

  В {{ ydb-short-name }} Go SDK корректная обработка ошибок закреплена в нескольких программных интерфейсах:

  {% cut "Функция повторов общего назначения" %}

  Основная логика обработки ошибок реализуется функцией-помощником `retry.Retry`
  Подробности выполнения повторных запросов максимально скрыты.
  Пользователь может влиять на логику работы функции `retry.Retry` двумя способами:
  * через контекст (можно устанавливать deadline и cancel);
  * через флаг идемпотентности операции `retry.WithIdempotent()`. По умолчанию операция считается неидемпотентной.

  Пользователь передает свою функцию в `retry.Retry`, которая по своей сигнатуре должна возвращать ошибку.
  В случае, если из пользовательской функции вернулся `nil`, то повторные запросы прекращаются.
  В случае, если из пользовательской функции вернулась ошибка, {{ ydb-short-name }} Go SDK пытается идентифицировать эту ошибку и в зависимости от нее выполняет повторные попытки.

  Пример кода, использующего функцию `retry.Retry`:
  ```golang
  package main

  import (
      "context"
      "time"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/retry"
  )

  func main() {
      db, err := ydb.Open(ctx,
          os.Getenv("YDB_CONNECTION_STRING"),
      )
      if err != nil {
          panic(err)
      }
      defer db.Close(ctx)
      var cancel context.CancelFunc
      // fix deadline for retries
      ctx, cancel := context.WithTimeout(ctx, time.Second)
      err = retry.Retry(
          ctx,
          func(ctx context.Context) error {
              whoAmI, err := db.Discovery().WhoAmI(ctx)
              if err != nil {
                  return err
              }
              fmt.Println(whoAmI)
          },
          retry.WithIdempotent(true),
      )
      if err != nil {
          panic(err)
      }
  }
  ```

  {% endcut %}

  {% cut "Выполнение повторных попыток при ошибках на объекте сессии {{ ydb-short-name }}" %}

  Для повторной обработки ошибок на уровне сессии сервиса таблиц {{ ydb-short-name }} есть функция `db.Table().Do(ctx, op)`, которая предоставляет подготовленную сессию для выполнения запросов.
  Функция `db.Table().Do(ctx, op)` использует пакет `retry`, а также следит за временем жизни сессий {{ ydb-short-name }}.
  Из пользовательской операции `op` согласно ее сигнатуре требуется возвращать ошибку или `nil`, чтобы драйвер смог по типу ошибки "понять" что нужно делать: поторять операцию или нет, с задержкой или нет, на этой же сессии или новой.
  Пользователь может влиять на логику выполнения повторных запросов через контекст и признак идемпотентности, а {{ ydb-short-name }} Go SDK интерпретирует возвращаемые из `op` ошибки.

  Пример кода, использующего функцию `db.Table().Do(ctx, op)`:
  ```golang
  err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
      desc, err = s.DescribeTableOptions(ctx)
      return
  }, table.WithIdempotent())
  if err != nil {
      return err
  }
  ```

  {% endcut %}

  {% cut "Выполнение повторных попыток при ошибках на объекте интерактивной транзакции {{ ydb-short-name }}" %}

  Для повторной обработки ошибок на уровне интерактивной транзакции сервиса таблиц {{ ydb-short-name }} есть функция `db.Table().DoTx(ctx, txOp)`, которая предоставляет подготовленную транзакцию {{ ydb-short-name }} на сессии для выполнения запросов.
  Функция `db.Table().DoTx(ctx, txOp)` использует пакет `retry`, а также следит за временем жизни сессий {{ ydb-short-name }}.
  Из пользовательской операции `txOp` согласно ее сигнатуре требуется возвращать ошибку или `nil`, чтобы драйвер смог по типу ошибки "понять" что нужно делать: поторять операцию или нет, с задержкой или нет, на этой же транзакции или новой.
  Пользователь может влиять на логику выполнения повторных запросов через контекст и признак идемпотентности, а {{ ydb-short-name }} Go SDK интерпретирует возвращаемые из `op` ошибки.

  Пример кода, использующего функцию `db.Table().DoTx(ctx, op)`:
  ```golang
  err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
      _, err := tx.Execute(ctx,
          "DECLARE $id AS Int64; INSERT INTO test (id, val) VALUES($id, 'asd')",
          table.NewQueryParameters(table.ValueParam("$id", types.Int64Value(100500))),
      )
      return err
  }, table.WithIdempotent())
  if err != nil {
      return err
  }
  ```

  {% endcut %}

  {% cut "Запросы к остальным сервисам {{ ydb-short-name }}" %}

  (`db.Scripting()`, `db.Scheme()`, `db.Coordination()`, `db.Ratelimiter()`, `db.Discovery()`) также используют внутри себя функцию `retry.Retry` для выполнения повторных запросов и не требуют использования внешних вспомогательных функций для повторов.

  {% endcut %}

- Go (database/sql)

  Стандартный пакет `database/sql` использует внутреннюю логику выполнения повторов на основе тех ошибок, что возвращает конкретная реализация драйвера.
  Так, в [коде](https://github.com/golang/go/tree/master/src/database/sql) пакета `database/sql` во многих местах можно встретить политику повторов из трех попыток:
  - 2 попытки выполнить на существующем соединении или новом (если пул соединений `database/sql` пуст)
  - 1 попытка выполнить на новом соединении.

  В большинстве случаев такой политики повторов достаточно, чтобы пережить временную недоступность нод {{ ydb-short-name }} или проблемы с сессией {{ ydb-short-name }}.

  {{ ydb-short-name }} Go SDK предоставляет специальные функции для гарантированного выполнения пользовательской операции:

  {% cut "Выполнение повторных попыток при ошибках на объекте соединения `*sql.Conn`:" %}

  Для повторной обработки ошибок  на объекте соединения `*sql.Conn` есть вспомогательная функция `retry.Do(ctx, db, op)`, которая предоставляет подготовленное соединение `*sql.Conn` для выполнения запросов.
  В функцию `retry.Do` требуется передать контекст, объект базы данных, а также пользовательскую операцию, которую требуется выполнить.
  Из клиентского кода можно влиять на логику выполнения повторных запросов через контекст и признак идемпотентности, а {{ ydb-short-name }} Go SDK в свою очередь интерпретирует возвращаемые из `op` ошибки.

  Пользовательская операция `op` должна возвращать ошибку или `nil`:
  - в случае, если из пользовательской функции вернулся `nil`, то повторные запросы прекращаются;
  - в случае, если из пользовательской функции вернулась ошибка, {{ ydb-short-name }} Go SDK пытается идентифицировать эту ошибку и в зависимости от нее предпринимает повторные попытки.

  Пример кода, использующего функцию `retry.Do`:
  ```golang
  import (
      "context"
      "database/sql"
      "fmt"
      "log"

      "github.com/ydb-platform/ydb-go-sdk/v3/retry"
  )

  func main() {
      ...
      err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
          row = cc.QueryRowContext(ctx, `
                  PRAGMA TablePathPrefix("/local");
                  DECLARE $seriesID AS Uint64;
                  DECLARE $seasonID AS Uint64;
                  DECLARE $episodeID AS Uint64;
                  SELECT views FROM episodes WHERE series_id = $seriesID AND season_id = $seasonID AND episode_id = $episodeID;
              `,
              sql.Named("seriesID", uint64(1)),
              sql.Named("seasonID", uint64(1)),
              sql.Named("episodeID", uint64(1)),
          )
          var views sql.NullFloat64
          if err = row.Scan(&views); err != nil {
              return fmt.Errorf("cannot scan views: %w", err)
          }
          if views.Valid {
              return fmt.Errorf("unexpected valid views: %v", views.Float64)
          }
          log.Printf("views = %v", views)
          return row.Err()
      }, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
      if err != nil {
          log.Printf("retry.Do failed: %v\n", err)
      }
  }
  ```

  {% endcut %}

  {% cut "Выполнение повторных попыток при ошибках на объекте интерактивной транзакции `*sql.Tx`:" %}

  Для повторной обработки ошибок  на объекте интерактивной транзакции `*sql.Tx` есть вспомогательная функция `retry.DoTx(ctx, db, op)`, которая предоставляет подготовленную транзакцию `*sql.Tx` для выполнения запросов.
  В функцию `retry.DoTx` требуется передать контекст, объект базы данных, а также пользовательскую операцию, которую требуется выполнить.
  В функцию приходит подготовленная транзакция `*sql.Tx`, на которой следует выполнять запросы к {{ ydb-short-name }}.
  Из клиентского кода можно влиять на логику выполнения повторных запросов через контекст и признак идемпотентности операции, а {{ ydb-short-name }} Go SDK в свою очередь интерпретирует возвращаемые из `op` ошибки.

  Пользовательская операция `op` должна возвращать ошибку или `nil`:
  - в случае, если из пользовательской функции вернулся `nil`, то повторные запросы прекращаются;
  - в случае, если из пользовательской функции вернулась ошибка, {{ ydb-short-name }} Go SDK пытается идентифицировать эту ошибку и в зависимости от нее предпринимает повторные попытки.

  Функция `retry.DoTx` использует режим изоляции read-write транзакции `sql.LevelDefault` по умолчанию, который можно изменить через опцию `retry.WithTxOptions`.

  Пример кода, использующего функцию `retry.Do`:
  ```golang
  import (
      "context"
      "database/sql"
      "fmt"
      "log"

      "github.com/ydb-platform/ydb-go-sdk/v3/retry"
  )

  func main() {
      ...
      err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
          row := tx.QueryRowContext(ctx,`
                  PRAGMA TablePathPrefix("/local");
                  DECLARE $seriesID AS Uint64;
                  DECLARE $seasonID AS Uint64;
                  DECLARE $episodeID AS Uint64;
                  SELECT views FROM episodes WHERE series_id = $seriesID AND season_id = $seasonID AND episode_id = $episodeID;
              `,
              sql.Named("seriesID", uint64(1)),
              sql.Named("seasonID", uint64(1)),
              sql.Named("episodeID", uint64(1)),
          )
          var views sql.NullFloat64
          if err = row.Scan(&views); err != nil {
              return fmt.Errorf("cannot select current views: %w", err)
          }
          if !views.Valid {
              return fmt.Errorf("unexpected invalid views: %v", views)
          }
          t.Logf("views = %v", views)
          if views.Float64 != 1 {
              return fmt.Errorf("unexpected views value: %v", views)
          }
          return nil
      }, retry.WithDoTxRetryOptions(retry.WithIdempotent(true)), retry.WithTxOptions(&sql.TxOptions{
          Isolation: sql.LevelSnapshot,
          ReadOnly:  true,
      }))
      if err != nil {
          log.Printf("do tx failed: %v\n", err)
      }
  }
  ```

  {% endcut %}

- Java

  В {{ ydb-short-name }} Java SDK механизм повторных запросов реализован в виде класс хелпера `SessionRetryContext`. Данный класс конструируется с помощью метода `SessionRetryContext.create` в который требуется передать реализацию интерфейса `SessionSupplier` - как правило это экземпляр класса `TableClient` или `QueryClient`.
  Дополнительно пользователь может задавать некоторые другие опции
  * `maxRetries(int maxRetries)` - максимальное количество повторов операции, не включает в себя первое выполение. Значение по умолчанию `10`
  * `retryNotFound(boolean retryNotFound)` - опция повтора операций, вернувших статус `NOT_FOUND`. По умолчанию включено.
  * `idempotent(boolean idempotent)` - признак идемпотентности операций. Идемпотентные операции будут повторяться для более широкого списка ошибок. По умолчанию отключено.

  Для запуска операций с ретраями класс `SessionRetryContext` предоставляет два метода
  * `CompletableFuture<Status> supplyStatus` - выполнение операции, возвращающей статус. В качестве аргумента принимает лямбду `Function<Session, CompletableFuture<Status>> fn`
  * `CompletableFuture<Result<T>> supplyResult` - выполнение операции, возвращающей данные. В качестве аргумента принимает лямбду `Function<Session, CompletableFuture<Result<T>>> fn`

  При использовании класса `SessionRetryContext` нужно учитывать, что повторное исполнение операции будет выполнятся в следующих случаях
  * Лямбда вернула [retryable](../../reference/ydb-sdk/error_handling.md) код ошибки
  * В рамках исполнения лямбды была вызвано `UnexpectedResultException` c [retryable](../../reference/ydb-sdk/error_handling.md) кодом ошибки



    {% cut "Пример кода, использующего SessionRetryContext.supplyStatus:" %}

      ```java
      private void createTable(TableClient tableClient, String database, String tableName) {
          SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
          TableDescription pets = TableDescription.newBuilder()
                  .addNullableColumn("species", PrimitiveType.Text)
                  .addNullableColumn("name", PrimitiveType.Text)
                  .addNullableColumn("color", PrimitiveType.Text)
                  .addNullableColumn("price", PrimitiveType.Float)
                  .setPrimaryKeys("species", "name")
                  .build();

          String tablePath = database + "/" + tableName;
          retryCtx.supplyStatus(session -> session.createTable(tablePath, pets))
                  .join().expectSuccess();
      }
      ```

    {% endcut %}

    {% cut "Пример кода, использующего SessionRetryContext.supplyResult:" %}

      ```java
      private void selectData(TableClient tableClient, String tableName) {
          SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
          String selectQuery
                  = "DECLARE $species AS Text;"
                  + "DECLARE $name AS Text;"
                  + "SELECT * FROM " + tableName + " "
                  + "WHERE species = $species AND name = $name;";

          Params params = Params.of(
                  "$species", PrimitiveValue.newText("cat"),
                  "$name", PrimitiveValue.newText("Tom")
          );

          DataQueryResult data = retryCtx
                  .supplyResult(session -> session.executeDataQuery(selectQuery, TxControl.onlineRo(), params))
                  .join().getValue();

          ResultSetReader rsReader = data.getResultSet(0);
          logger.info("Result of select query:");
          while (rsReader.next()) {
              logger.info("  species: {}, name: {}, color: {}, price: {}",
                      rsReader.getColumn("species").getText(),
                      rsReader.getColumn("name").getText(),
                      rsReader.getColumn("color").getText(),
                      rsReader.getColumn("price").getFloat()
              );
          }
      }
      ```

    {% endcut %}

{% endlist %}
