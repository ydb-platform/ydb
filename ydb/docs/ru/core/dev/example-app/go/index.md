# Приложение на Go

На этой странице подробно разбирается код [тестового приложения](https://github.com/ydb-platform/ydb-go-examples/tree/master/basic), использующего [Go SDK](https://github.com/ydb-platform/ydb-go-sdk/v3) {{ ydb-short-name }}.

## Скачивание и запуск {#download}

Приведенный ниже сценарий запуска использует [git](https://git-scm.com/downloads) и [Go](https://go.dev/doc/install). Предварительно должен быть установлен [YDB Go SDK](../../../reference/ydb-sdk/install.md).

Создайте рабочую директорию и выполните в ней из командной строки команду клонирования репозитория с github.com:

``` bash
git clone git@github.com:ydb-platform/ydb-go-sdk.git
```

Далее из этой же рабочей директории выполните команду запуска тестового приложения, которая будет отличаться в зависимости от того, к какой базе данных необходимо подключиться.

{% include [run_options.md](_includes/run_options.md) %}



{% include [init.md](../_includes/steps/01_init.md) %}

Для работы с `YDB` в `go` следует импортировать пакет драйвера `ydb-go-sdk`:

```go
import (
 "context"
 "log"
 "path"

 "github.com/ydb-platform/ydb-go-sdk/v3"
 "github.com/ydb-platform/ydb-go-sdk/v3/query"
)
```

Для взаимодействия с YDB необходимо создать объект подключения к YDB:

```go
db, err := ydb.Open(context.Background(), "grpc://localhost:2136/local")
if err != nil {
  // обработка ошибки подключения
}
```

Метод `ydb.Open` возвращает в случае успеха экземпляр драйвера, которые выполняет ряд служебных функций, таких как актуализация сведений о кластере YDB и клиентская балансировка.

Метод `ydb.Open` принимает 2 обязательных аргемента:

* контекст
* строка подключения к YDB.

Также доступно множество опций подключения, позволяющих переопределить значения по умолчанию.

Так, по умолчанию используется анонимная авторизация. А для подключения к кластеру YDB с использованием токена авторизации будет иметь вид:

```go
db, err := ydb.Open(context.Background(), clusterEndpoint,
 ydb.WithAccessTokenCredentials(token),
)
```

Полный список провайдеров авторизации приведен в [документации ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk?tab=readme-ov-file#credentials-) и в разделе [рецептов](../../../recipes/ydb-sdk/auth.md)

В конце работы приложения для очистки ресурсов следует закрыть драйвер

```go
defer db.Close(ctx)
```

Объект `db` является входной точкой для работы со всеми сервисами `YDB`:

* `db.Query()` - клиент query-сервиса
* `db.Table()` - клиент table-сервиса
* `db.Discovery()` - клиент discovery-сервиса
* `db.Topic()` - клиент topic-сервиса
* `db.Coordination()` - клиент coordination-сервиса
* `db.Ratelimiter()` - клиент ratelimiter-сервиса
* `db.Scripting()` - клиент scriptingYQL-сервиса
* `db.Scheme()` - клиент scheme-сервиса
* `db.Operation()` - клиент operation-сервиса

Для работы таблицами YDB следует использовать клиента query-сервиса `db.Query()`. Выполнение `DDL`, `DML`, `DQL` и `TCL` запросов в таблицы YDB осуществляется на специальных объекта - сессиях `query.Session`. Сессии YDB хранят контекст выполнения запросов (например, Prepared statements и транзакции) и позволяют осуществлять серверную балансировать балансировку нагрузки на узлы кластера YDB.

Клиент query-сервиса предоставляет `API` для выполнения запросов над таблицами:

* метод `db.Query().Do(ctx, op)` реализует фоновое создание сессий и повторные попытки выполнить пользовательскую операцию `op func(ctx context.Context, s query.Session) error`, в которую пользовательскому коду отдается подготовленная сессия `query.Session`.
* метод `db.Query().DoTx(ctx, op)` принимает пользовательскую операцию `op func(ctx context.Context, tx query.TxActor) error`, в которую пользовательскому коду отдается подготовленная (заранее открытая) транзакция `query.TxActor`. `Commit` транзакции также автоматический, если из пользовательской операции возвращается `nil`. В случае, если из пользовательской операции возвращается ошибка, то для текущей транзакции автоматически будет вызван `Rollback`.
* метод `db.Query().Exec` является вспомогательным и предназначен для выполнения единичного запроса **без результата** с автоматическими повторными попытками. Метод `Exec` возвращает `nil` в случае успешного выполнения запроса и ошибку, если операция не удалась.
* метод `db.Query().Query` является вспомогательным и предназначен для выполнения единичного запроса с автоматическими повторными попытками внутри. Метод `Query` возвращает в случае успеха материализованный результат запроса `query.Result`, позволяет итерироваться по вложенным спискам строк `query.ResultSet`. Для широких SQL-запросов, возвращающих большое количество строк, материализация результата может привести к проблеме [OOM killed](https://en.wikipedia.org/wiki/Out_of_memory).
* метод `db.Query().QueryResultSet` является вспомогательным и предназначен для выполнения единичного запроса с автоматическими повторными попытками внутри. Метод `QueryResultSet` возвращает в случае успеха материализованный список результатов `query.ResultSet`. Для широких SQL-запросов, возвращающих большое количество строк, материализация результата может привести к проблеме [OOM killed](https://en.wikipedia.org/wiki/Out_of_memory).
* метод `db.Query().QueryRow` является вспомогательным и предназначен для выполнения единичного запроса с автоматическими повторными попытками внутри. Метод `QueryRow` возвращает в случае успеха единственную строку `query.Row`.

{% include [steps/02_create_table.md](../_includes/steps/02_create_table.md) %}

Пример создания таблицы:

```go
import "github.com/ydb-platform/ydb-go-sdk/v3/query"
////////////////////////////////////////////////////
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

Для выполнения YQL-запросов без результата `query.Result` используется метод `query.Session.Exec`. Для выполнения YQL-запросов c результатом используется методы `query.Session.Query`, `query.Session.QueryResultSet` и `query.Session.QueryRow`.

SDK позволяет в явном виде контролировать выполнение транзакций и настраивать необходимый режим выполнения транзакций с помощью структуры `query.TxControl`.

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

Чтобы вычитать данные строки YDB `query.Row` используются методы `query.Row.Scan` (по индексу колонки), `query.Row.ScanNamed` (по названию колонки) и `query.Row.ScanStruct` (по названиям колонок, зафиксированных к тегах структуры):

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

{% include [scan_query.md](../_includes/steps/08_scan_query.md) %}

Если ожидаемое количество данных от запроса велико, не следует пытаться загружать их полностью в оперативную память с помощью вспомогательных методов клиента Query-сервиса, таких как `query.Client.Query` и `query.Client.QueryResultSet`. Данные вспомогательные методы реализуют внутренние повторные попытки и отдают уже материализованный результат. При большом количестве возвращаемых строк материализация результата может привести к известной проблеме "OOM killed".

Для запросов, предполагающих большое количество данных, следует пользоваться методами сессии YDB `query.Session.Query` или `query.Session.QueryResultSet`, которые отдают "потоковый" результат без материализации. Важно отметить, что сессия `query.Session` доступна только из метода `query.Client.Do`, реализующего механизмы выполнения повторных попыток при ошибках. Соответственно, необходимо учитывать, что чтение может быть прервано в любой момент, и в таком случае весь процесс выполнения запроса начнётся заново.

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
