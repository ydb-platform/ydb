# Приложение на Go

На этой странице подробно разбирается код [тестового приложения](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/basic), использующего [Go SDK](https://github.com/ydb-platform/ydb-go-sdk/v3) {{ ydb-short-name }}.

{% include [addition.md](auxilary/addition.md) %}

{% include [init.md](steps/01_init.md) %}

Для работы с `YDB` в `go` следует импортировать пакет драйвера `ydb-go-sdk`:

```go
import (
  // общие импорты
  "context"
  "path"

  // импорты пакетов ydb-go-sdk
  "github.com/ydb-platform/ydb-go-sdk/v3"
  "github.com/ydb-platform/ydb-go-sdk/v3/table" // для работы с table-сервисом
  "github.com/ydb-platform/ydb-go-sdk/v3/table/options" // для работы с table-сервисом
  "github.com/ydb-platform/ydb-go-sdk-auth-environ" // для аутентификации с использованием перменных окружения
  "github.com/ydb-platform/ydb-go-yc" // для работы с YDB в Яндекс Облаке
)
```

Фрагмент кода приложения для инициализации драйвера:

```go
ctx := context.Background()
// строка подключения
dsn := "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g8skpblkos03malf3s/etn01f8gv9an9sedo9fu"
// IAM-токен
token := "t1.9euelZrOy8aVmZKJm5HGjceMkMeVj-..."
// создаем объект подключения db, является входной точкой для сервисов YDB
db, err := ydb.New(
  ctx,
  ydb.WithConnectionString(dsn),
//  yc.WithInternalCA(), // используем сертификаты Яндекс Облака
  ydb.WithAccessTokenCredentials(token), // аутентификация с помощью токена
//  ydb.WithAnonimousCredentials(token), // анонимная аутентификация (например, в docker ydb)
//  yc.WithMetadataCredentials(token), // аутентификация изнутри виртуальной машины в Яндекс Облаке или из Яндекс Функции
//  yc.WithServiceAccountKeyFileCredentials("~/.ydb/sa.json"), // аутентификация в Яндекс Облаке с помощью файла сервисного аккаунта
//  environ.WithEnvironCredentials(ctx), // аутентификация с использованием переменных окружения
)
if err != nil {
  // обработка ошибки подключения
}
// закрытие драйвера по окончании работы программы обязательно
defer func() {
  _ = db.Close(ctx)
}
```

Объект `db` является входной точкой для работы с сервисами `YDB`.
Для работы сервисом таблиц следует использовать клиента table-сервиса `db.Table()`.
Клиент table-сервиса предоставляет `API` для выполнения запросов над таблицами.
Наиболее востребован метод `db.Table().Do(ctx, op)`, который реализует фоновое создание сессий и повторные попытки выполнить пользовательскую операцию `op`, в которую пользовательскому коду отдается подготовленная сессия. 
Сессия имеет исчерпывающее `API`, позволяющее выполнять `DDL`, `DML`, `DQL` и `TCL` запросы.

## Создание таблиц с помощью CreateTable API {#create-table-api}

Для создания таблиц используется метод `table.Session.CreateTable()`:

```go
err = db.Table().Do(
  ctx,
  func(ctx context.Context, s table.Session) (err error) {
    return s.CreateTable(ctx, path.Join(db.Name(), "series"),
      options.WithColumn("series_id", types.Optional(types.TypeUint64)),
      options.WithColumn("title", types.Optional(types.TypeUTF8)),
      options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
      options.WithColumn("release_date", types.Optional(types.TypeDate)),
      options.WithColumn("comment", types.Optional(types.TypeUTF8)),
      options.WithPrimaryKeyColumn("series_id"),
    )
  },
)
if err != nil {
  // обработка ситуации, когда не удалось выполнить запрос
}
```

С помощью метода `table.Session.DescribeTable()` можно вывести информацию о структуре таблицы и убедиться, что она успешно создалась:

```go
err = db.Table().Do(
  ctx,
  func(ctx context.Context, s table.Session) (err error) {
    desc, err := s.DescribeTable(ctx, path.Join(db.Name(), "series"))
    if err != nil {
      return
    }
    log.Printf("> describe table: %s\n", tableName)
    for _, c := range desc.Columns {
      log.Printf("  > column, name: %s, %s\n", c.Type, c.Name)
    }
    return
  }
)
if err != nil {
  // обработка ситуации, когда не удалось выполнить запрос
}
```

## Обработка запросов и транзакций {#query-processing}

Для выполнения YQL-запросов используется метод `table.Session.Execute()`.
SDK позволяет в явном виде контролировать выполнение транзакций и настраивать необходимый режим выполнения транзакций с помощью структуры `table.TxControl`.

```go
var (
  readTx = table.TxControl(
    table.BeginTx(
      table.WithOnlineReadOnly(),
    ),
    table.CommitTx(),
  )
)
err := db.Table().Do(
  ctx,
  func(ctx context.Context, s table.Session) (err error) {
    var (
      res   result.Result
      id    *uint64 // указатель - для опциональных результатов
      title *string // указатель - для опциональных результатов
      date  *time.Time // указатель - для опциональных результатов
    )
    _, res, err = s.Execute(
      ctx, 
      readTx, 
      `
        DECLARE $seriesID AS Uint64;
        SELECT
          series_id,
          title,
          release_date
        FROM
          series
        WHERE
          series_id = $seriesID;
      `,
      table.NewQueryParameters(
        table.ValueParam("$seriesID", types.Uint64Value(1)), // подстановка в условие запроса
      ),
      options.WithQueryCachePolicy(
        options.WithQueryCachePolicyKeepInCache(), // включаем серверный кэш скомпилированных запросов
      ),
    )
    if err != nil {
      return err
    }
    defer func() {
      _ = res.Close() // закрытие result'а обязательно
    }()
    log.Printf("> select_simple_transaction:\n")
    // Имена колонок в NextResultSet устанавливают порядок чтения колонок в последующем Scan
    for res.NextResultSet(ctx, "series_id", "title", "release_date") {
      for res.NextRow() {
        // в Scan передаем адреса (и типы данных), куда следует присвоить результаты запроса
        err = res.Scan(&id, &title, &date)
        if err != nil {
          return err
        }
        log.Printf(
          "  > %d %s %s\n",
          *id, *title, *date,
        )
      }
    }
    return res.Err()
  },
)
if err != nil {
  // обработка ошибки выполнения запроса
}
```

{% include [scan_query.md](steps/08_scan_query.md) %}

Для выполнения scan-запросов используется метод `table.Session.StreamExecuteScanQuery()`.

```go
var (
  query = `
    DECLARE $series AS List<UInt64>;
    SELECT series_id, season_id, title, first_aired
    FROM seasons
    WHERE series_id IN $series
  `
  res result.StreamResult
)
err = c.Do(
  ctx,
  func(ctx context.Context, s table.Session) (err error) {
    res, err = s.StreamExecuteScanQuery(ctx, query,
      table.NewQueryParameters(
        table.ValueParam("$series",
          types.ListValue(
            types.Uint64Value(1),
            types.Uint64Value(10),
          ),
        ),
      ),
    )
    if err != nil {
      return err
    }
    defer func() {
      _ = res.Close() // закрытие result'а обязательно
    }()
    var (
      seriesID uint64
      seasonID uint64
      title    string
      date     time.Time
    )
    log.Print("\n> scan_query_select:")
    // Имена колонок в NextResultSet устанавливают порядок чтения колонок в последующем ScanWithDefaults
    for res.NextResultSet(ctx, "series_id", "season_id", "title", "first_aired") {
      if err = res.Err(); err != nil {
        return err
      }
      for res.NextRow() {
        // ScanWithDefaults позволяет "развернуть" опциональные результаты или использовать дефолтное значение типа go
        err = res.ScanWithDefaults(&seriesID, &seasonID, &title, &date)
        if err != nil {
          return err
        }
        log.Printf("#  Season, SeriesId: %d, SeasonId: %d, Title: %s, Air date: %s", seriesID, seasonID, title, date)
      }
    }
    return res.Err()
  },
)
if err != nil {
  // обработка ошибки выполнения запроса
}
```

{% include [error_handling.md](steps/50_error_handling.md) %}

{% note info %}

Разбор кода тестового приложения, использующего архивные версии Go SDK:
 - [github.com/yandex-cloud/ydb-go-sdk](https://github.com/yandex-cloud/ydb-go-sdk/tree/v1.5.1) доступен по [ссылке](../archive/example-go-v1.md),
 - [github.com/yandex-cloud/ydb-go-sdk/v2](https://github.com/yandex-cloud/ydb-go-sdk/tree/v2.11.2) доступен по [ссылке](../archive/example-go-v2.md).

{% endnote %}
