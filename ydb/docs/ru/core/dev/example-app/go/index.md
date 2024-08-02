# Приложение на Go

На этой странице подробно разбирается код [тестового приложения](https://github.com/ydb-platform/ydb-go-examples/tree/master/basic), использующего [Go SDK](https://github.com/ydb-platform/ydb-go-sdk/v3) {{ ydb-short-name }}.

## Скачивание и запуск {#download}

Приведенный ниже сценарий запуска использует [git](https://git-scm.com/downloads) и [Go](https://go.dev/doc/install). Предварительно должен быть установлен [YDB Go SDK](../../../reference/ydb-sdk/install.md).

Создайте рабочую директорию и выполните в ней из командной строки команду клонирования репозитория с github.com:

``` bash
git clone https://github.com/ydb-platform/ydb-go-examples/
```

Далее из этой же рабочей директории выполните команду запуска тестового приложения, которая будет отличаться в зависимости от того, к какой базе данных необходимо подключиться.

{% include [run_options.md](_includes/run_options.md) %}



{% include [init.md](../_includes/steps/01_init.md) %}

Для работы с `YDB` в `go` следует импортировать пакет драйвера `ydb-go-sdk`:

```go
import (
  // общие импорты из стандартной библиотеки
  "context"
  "log"
  "path"

  // импорты пакетов ydb-go-sdk
  "github.com/ydb-platform/ydb-go-sdk/v3"
  "github.com/ydb-platform/ydb-go-sdk/v3/table" // для работы с table-сервисом
  "github.com/ydb-platform/ydb-go-sdk/v3/table/options" // для работы с table-сервисом
  "github.com/ydb-platform/ydb-go-sdk/v3/table/result" // для работы с table-сервисом
  "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named" // для работы с table-сервисом
  "github.com/ydb-platform/ydb-go-sdk/v3/table/types" // для работы с типами YDB и значениями
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
db, err := ydb.Open(ctx, dsn,
//  yc.WithInternalCA(), // используем сертификаты Яндекс Облака
  ydb.WithAccessTokenCredentials(token), // аутентификация с помощью токена
//  ydb.WithAnonimousCredentials(), // анонимная аутентификация (например, в docker ydb)
//  yc.WithMetadataCredentials(token), // аутентификация изнутри виртуальной машины в Яндекс Облаке или из Яндекс Функции
//  yc.WithServiceAccountKeyFileCredentials("~/.ydb/sa.json"), // аутентификация в Яндекс Облаке с помощью файла сервисного аккаунта
//  environ.WithEnvironCredentials(ctx), // аутентификация с использованием переменных окружения
)
if err != nil {
  // обработка ошибки подключения
}
// закрытие драйвера по окончании работы программы обязательно
defer db.Close(ctx)
```

Объект `db` является входной точкой для работы с сервисами `YDB`.
Для работы сервисом таблиц следует использовать клиента table-сервиса `db.Table()`.
Клиент table-сервиса предоставляет `API` для выполнения запросов над таблицами.
Наиболее востребован метод `db.Table().Do(ctx, op)`, который реализует фоновое создание сессий и повторные попытки выполнить пользовательскую операцию `op`, в которую пользовательскому коду отдается подготовленная сессия.
Сессия имеет исчерпывающее `API`, позволяющее выполнять `DDL`, `DML`, `DQL` и `TCL` запросы.

{% include [steps/02_create_table.md](../_includes/steps/02_create_table.md) %}

Для создания таблиц используется метод `table.Session.CreateTable()`:

```go
err = db.Table().Do(ctx,
  func(ctx context.Context, s table.Session) (err error) {
    return s.CreateTable(ctx, path.Join(db.Name(), "series"),
      options.WithColumn("series_id", types.TypeUint64),  // not null column
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
err = db.Table().Do(ctx,
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
  },
)
if err != nil {
  // обработка ситуации, когда не удалось выполнить запрос
}
```

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

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
err := db.Table().Do(ctx,
  func(ctx context.Context, s table.Session) (err error) {
    var (
      res   result.Result
      id    uint64 // переменная для required результатов
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
    )
    if err != nil {
      return err
    }
    defer res.Close() // закрытие result'а обязательно
    log.Printf("> select_simple_transaction:\n")
    for res.NextResultSet(ctx) {
      for res.NextRow() {
        // в ScanNamed передаем имена колонок из строки сканирования,
        // адреса (и типы данных), куда следует присвоить результаты запроса
        err = res.ScanNamed(
          named.Optional("series_id", &id),
          named.Optional("title", &title),
          named.Optional("release_date", &date),
        )
        if err != nil {
          return err
        }
        log.Printf(
          "  > %d %s %s\n",
          id, *title, *date,
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

{% include [scan_query.md](../_includes/steps/08_scan_query.md) %}

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
err = c.Do(ctx,
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
    defer res.Close() // закрытие result'а обязательно
    var (
      seriesID uint64
      seasonID uint64
      title    string
      date     time.Time
    )
    log.Print("\n> scan_query_select:")
    for res.NextResultSet(ctx) {
      if err = res.Err(); err != nil {
        return err
      }
      for res.NextRow() {
        // named.OptionalWithDefault позволяет "развернуть" опциональные
        // результаты или использовать дефолтное значение типа go
        err = res.ScanNamed(
          named.Required("series_id", &seriesID),
          named.OptionalWithDefault("season_id", &seasonID),
          named.OptionalWithDefault("title", &title),
          named.OptionalWithDefault("first_aired", &date),
        )
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
