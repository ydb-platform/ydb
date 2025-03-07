# Приложение на Go

<!-- markdownlint-disable blanks-around-fences -->

На этой странице подробно разбирается код [тестового приложения](https://github.com/ydb-platform/ydb-go-examples/tree/master/basic), использующего {{ ydb-short-name }} [Go SDK](https://github.com/ydb-platform/ydb-go-sdk).

## Скачивание и запуск {#download}

Приведенный ниже сценарий запуска использует [git](https://git-scm.com/downloads) и [Go](https://go.dev/doc/install). Предварительно должен быть установлен [{{ ydb-short-name }} Go SDK](../../../reference/ydb-sdk/install.md).

Создайте рабочую директорию и выполните в ней из командной строки команду клонирования репозитория с GitHub:

``` bash
git clone https://github.com/ydb-platform/ydb-go-sdk.git
```

Далее из этой же рабочей директории выполните команду запуска тестового приложения, которая будет отличаться в зависимости от того, к какой базе данных необходимо подключиться.

{% include [run_options.md](_includes/run_options.md) %}

{% include [init.md](../_includes/steps/01_init.md) %}

Для работы с {{ ydb-short-name }} в `go` следует импортировать пакет драйвера `ydb-go-sdk`:

```go
import (
 "context"
 "log"
 "path"

 "github.com/ydb-platform/ydb-go-sdk/v3"
 "github.com/ydb-platform/ydb-go-sdk/v3/query"
)
```

Для взаимодействия с {{ ydb-short-name }} необходимо создать экземляр {{ ydb-short-name }}-драйвера:


```go
db, err := ydb.Open(context.Background(), "grpc://localhost:2136/local")
if err != nil {
  // обработка ошибки подключения
}

// При заверщении работы с базой (например, выходе из программы) закройте драйвер
defer db.Close(context.Background())
```

Метод `ydb.Open` возвращает в случае успеха экземпляр драйвера, который выполняет ряд служебных функций, таких как актуализация сведений о кластере {{ ydb-short-name }} и клиентская балансировка.

Метод `ydb.Open` принимает два обязательных аргумента:

* контекст;
* строка подключения к {{ ydb-short-name }}.

Также доступно множество опций подключения, позволяющих переопределить значения по умолчанию.

По умолчанию используется анонимная аутентификация. А подключение к кластеру {{ ydb-short-name }} с использованием токена будет иметь следующий вид:

```go
db, err := ydb.Open(context.Background(), clusterEndpoint,
 ydb.WithAccessTokenCredentials(token),
)
```

Полный список провайдеров аутентификации приведён в [документации ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk?tab=readme-ov-file#credentials-) и в разделе [рецептов](../../../recipes/ydb-sdk/auth.md).

В конце работы приложения для очистки ресурсов следует закрыть драйвер:

```go
defer db.Close(ctx)
```

Объект `db` является входной точкой для работы с {{ ydb-short-name }}, а для запросов к таблицам используется Query-сервис `db.Query()`.

Выполнение YQL-запросов осуществляется на специальных объектах — сессиях `query.Session`. Сессии хранят контекст выполнения запросов (например, транзакции) и позволяют осуществлять серверную балансировку нагрузки на узлы кластера {{ ydb-short-name }}.

Клиент Query-сервиса предоставляет API для выполнения запросов к таблицам:

* Метод `db.Query().Do(ctx, op)` реализует фоновое создание сессий и повторные попытки выполнить пользовательскую операцию `op func(ctx context.Context, s query.Session) error`, в которую пользовательскому коду передаётся подготовленная сессия `query.Session`.
* Метод `db.Query().DoTx(ctx, op)` принимает пользовательскую операцию `op func(ctx context.Context, tx query.TxActor) error`, в которую пользовательскому коду передаётся подготовленная (заранее открытая) транзакция `query.TxActor`. Автоматическое выполнение `Commit` транзакции происходит, если из пользовательской операции возвращается `nil`. В случае возврата ошибки для текущей транзакции автоматически вызывается `Rollback`.
* Метод `db.Query().Exec` является вспомогательным и предназначен для выполнения единичного запроса **без результата** с автоматическими повторными попытками. Метод `Exec` возвращает `nil` в случае успешного выполнения запроса и ошибку, если операция не удалась.
* Метод `db.Query().Query` является вспомогательным и предназначен для выполнения единичного запроса с повторными попытками при необходимости. Текст запроса может содержать несколько выражений с результатами. Метод `Query` возвращает, в случае успеха, материализованный результат запроса (все данные уже прочитаны с сервера и доступны в локальной памяти) `query.Result` и позволяет итерироваться по вложенным спискам строк `query.ResultSet`. Для широких SQL-запросов, возвращающих большое количество строк, материализация результата может привести к проблеме [OOM](https://en.wikipedia.org/wiki/Out_of_memory).
* Метод `db.Query().QueryResultSet` является вспомогательным и предназначен для выполнения единичного запроса с повторными попытками при необходимости. В запросе должно быть ровно одно выражение, возвращающее результат (дополнительно могут присутствовать выражения, не возвращающие результат, например `UPSERT`). Метод `QueryResultSet` возвращает, в случае успеха, материализованный список результатов `query.ResultSet`. Для широких SQL-запросов, возвращающих большое количество строк, материализация результата может привести к проблеме [OOM](https://en.wikipedia.org/wiki/Out_of_memory).
* Метод `db.Query().QueryRow` является вспомогательным и предназначен для выполнения единичного запроса с повторными попытками при необходимости. Метод `QueryRow` возвращает, в случае успеха, единственную строку `query.Row`.

{% include [steps/02_create_table.md](../_includes/steps/02_create_table.md) %}

Пример запроса без возвращаемого результата (создание таблицы):

```go
import "github.com/ydb-platform/ydb-go-sdk/v3/query"

err = db.Query().Exec(ctx, `
 CREATE TABLE IF NOT EXISTS series (
  series_id Bytes,
  title Text,
  series_info Text,
  release_date Date,
  comment Text,

  PRIMARY KEY(series_id)
 )`, query.WithTxControl(query.NoTx()),
)
if err != nil {
  // обработка ошибки выполнения запроса
}
```

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

Для выполнения YQL-запросов и чтения результатов используются методы `query.Session.Query`, `query.Session.QueryResultSet` и `query.Session.QueryRow`.

SDK позволяет явно контролировать выполнение транзакций и настраивать необходимый режим выполнения транзакций с помощью структуры `query.TxControl`.

```go
readTx := query.TxControl(
 query.BeginTx(
  query.WithSnapshotReadOnly(),
 ),
 query.CommitTx(),
)
row, err := db.Query().QueryRow(ctx,`
 DECLARE $seriesID AS Uint64;
 SELECT
   series_id,
   title,
   release_date
 FROM
   series
 WHERE
   series_id = $seriesID;`,
 query.WithParameters(
  ydb.ParamsBuilder().Param("$seriesID").Uint64(1).Build(),
 ),
 query.WithTxControl(readTx),
)
if err != nil {
  // обработка ошибки выполнения запроса
}
```

Для получения данных строки `query.Row` можно использовать следующие методы:

* `query.Row.ScanStruct` — по названиям колонок, зафиксированным в тегах структуры.
* `query.Row.ScanNamed` — по названиям колонок.
* `query.Row.Scan` — по порядку колонок.

{% list tabs %}

- ScanStruct

  ```go
  var info struct {
   SeriesID    string    `sql:"series_id"`
   Title       string    `sql:"title"`
   ReleaseDate time.Time `sql:"release_date"`
  }
  err = row.ScanStruct(&info)
  if err != nil {
    // обработка ошибки выполнения запроса
  }
  ```

- ScanNamed

  ```go
  var seriesID, title string
  var releaseDate time.Time
  err = row.ScanNamed(query.Named("series_id", &seriesID), query.Named("title", &title), query.Named("release_date", &releaseDate))
  if err != nil {
    // обработка ошибки выполнения запроса
  }
  ```

- Scan

  ```go
  var seriesID, title string
  var releaseDate time.Time
  err = row.Scan(&seriesID, &title, &releaseDate)
  if err != nil {
    // обработка ошибки выполнения запроса
  }
  ```
  
 {% endlist %}

{% include [scan_query.md](../_includes/steps/08_scan_query.md) %}

{% note warning %}

Если ожидаемый объём данных от запроса велик, не следует пытаться загружать их полностью в оперативную память с помощью вспомогательных методов, таких как `query.Client.Query` и `query.Client.QueryResultSet`. Эти методы возвращают уже материализованный результат, при котором все данные запроса загружены с сервера в локальную память клиентского приложения. При большом количестве возвращаемых строк материализация результата может привести к проблеме [OOM](https://en.wikipedia.org/wiki/Out_of_memory).

Для таких запросов следует использовать методы `query.TxActor.Query` или `query.TxActor.QueryResultSet` на сессии или транзакции, которые предоставляют итератор по результату без полной материализации. Сессия `query.Session` доступна только из метода `query.Client.Do`, реализующего механизмы повторных попыток при ошибках. Нужно учитывать, что чтение может быть прервано в любой момент, и в таком случае весь процесс выполнения запроса начнётся заново. То есть функция, переданная в `Do`, может вызываться более одного раза.

{% endnote %}


```go
err = db.Query().Do(ctx,
 func(ctx context.Context, s query.Session) error {
  rows, err := s.QueryResultSet(ctx,`
   SELECT series_id, season_id, title, first_aired
   FROM seasons`,
  )
  if err != nil {
   return err
  }
  defer rows.Close(ctx)
  for row, err := range rows.Rows(ctx) {
   if err != nil {
    return err
   }
   var info struct {
    SeriesID    string    `sql:"series_id"`
    SeasonID    string    `sql:"season_id"`
    Title       string    `sql:"title"`
    FirstAired  time.Time `sql:"first_aired"`
   }
   err = row.ScanStruct(&info)
   if err != nil {
    return err
   }
   fmt.Printf("%+v\n", info)
  }
  return nil
 },
 query.WithIdempotent(),
)
if err != nil {
  // обработка ошибки выполнения запроса
}
```
