# Пользовательские атрибуты таблиц

Вы можете хранить произвольную информацию в [пользовательских атрибутах](../concepts/datamodel/table.md#users-attr) и обрабатывать ее в приложении, а также с помощью CLI.

Например, приложение использует таблицу в БД. В пользовательских атрибутах хранится версия схемы таблицы. При изменении схемы таблицы необходимо выполнить миграцию данных. Чтобы приложение при старте не применило миграцию, которая уже была выполнена, а также не забыло про миграцию, которую нужно применить, нужно проверить версию схемы в пользовательских атрибутах:

* В атрибуте версия `1`, при этом наше приложение умеет работать с версией `2` — применяем миграцию `1`>`2`, обновляем значение атрибута на `2`.
* В атрибуте версия `2`, наше приложение умеет работать с версией `2` — миграция не нужна.
* В атрибуте версия `2`, наше приложение умеет работать с версией `1` — завершаем работу аварийно с уведомлением о попытке запуска старой версии приложения на новой версии схемы данных.

С применением пользовательских атрибутов отпадает необходимость хранить версию схемы в отдельной таблице.

## Установите пользовательский атрибут при создании таблицы {#create}

{% list tabs %}

- Go

  Чтобы установить пользовательский атрибут при создании таблицы `series`, передайте ключ `scheme_version` и значение атрибута `1` в опции `options.WithAttribute` метода `CreateTable`:

  ```go
  err := client.Do(ctx, func(ctx context.Context, s table.Session) error {
    return s.CreateTable(ctx, "episodes",
      options.WithColumn("series_id", types.Optional(types.TypeUint64)),
      options.WithColumn("season_id", types.Optional(types.TypeUint64)),
      options.WithColumn("episode_id", types.Optional(types.TypeUint64)),
      options.WithColumn("title", types.Optional(types.TypeText)),
      options.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
      options.WithAttribute("scheme_version", "1"),
    )
  })
  ```

{% endlist %}

## Установите пользовательский атрибут для существующей таблицы {#add}

{% list tabs %}

- Go

  Чтобы установить пользовательский атрибут для существующей таблицы `series`, передайте ключ `scheme_version` и значение атрибута `1` в опции `options.WithAddAttribute` метода `AlterTable`:

  ```go
  err = db.Table().Do(ctx,
    func(ctx context.Context, s table.Session) (err error) {
      return s.AlterTable(ctx, path.Join(db.Name(), "series"),
        options.WithAddAttribute("scheme_version", "1"),
      )
    },
  )
  ```

- CLI

  Чтобы установить пользовательский атрибут для существующей таблицы `series`, передайте ключ `scheme_version` и значение атрибута `1` в опции `--attribute` команды [ydb table attribute add](../reference/ydb-cli/table-attribute-add.md):

  ```bash
  ydb table attribute add --attribute scheme_version=1 series
  ```

{% endlist %}

## Измените пользовательский атрибут {#alter}

{% list tabs %}

- Go

  Чтобы изменить пользовательский атрибут при изменении схемы таблицы, передайте ключ `scheme_version` и новое значение атрибута `2` в опции `WithAlterAttribute` метода `AlterTable`:

  ```go
  err = db.Table().Do(ctx,
    func(ctx context.Context, s table.Session) (err error) {
      return s.AlterTable(ctx, path.Join(db.Name(), "series"),
        options.WithAddColumn("air_date", types.Optional(types.TypeUint64)),
        options.WithAlterAttribute("scheme_version", "2"),
      )
    },
  )
  ```

- CLI

  Чтобы изменить пользовательский атрибут для существующей таблицы `series`, передайте ключ `scheme_version` и значение атрибута `2` в опции `--attribute` команды [ydb table attribute add](../reference/ydb-cli/table-attribute-add.md):

  ```bash
  ydb table attribute add --attribute scheme_version=2 series
  ```

{% endlist %}

## Просмотрите пользовательские атрибуты {#view}

{% list tabs %}

- Go

  Чтобы получить информацию о схеме таблицы `series` включая данные о пользовательских атрибутах, используйте метод `table.Session.DescribeTable()`:

  ```go
  err := c.Do(ctx,
    func(ctx context.Context, s table.Session) error {
      description, err := s.DescribeTable(ctx, path.Join(prefix, "series"))
      if err != nil {
        return err
      }
      for k, v := range description.Attributes {
        log.Println(k, "=", v)
      }
      return nil
    },
  )
  ```

- CLI

  Чтобы получить информацию о схеме таблицы `series` включая данные о пользовательских атрибутах, используйте команду [ydb scheme describe](../reference/ydb-cli/commands/scheme-describe.md):

  ```bash
  ydb scheme describe series
  ```

  Результат:

  ```text
  ...
  Attributes:
  ┌────────────────┬───────┐
  | Name           | Value |
  ├────────────────┼───────┤
  | scheme_version | 2     |
  └────────────────┴───────┘
  ...
  ```

{% endlist %}

## Удалите пользовательский атрибут {#drop}

{% list tabs %}

- Go

  Чтобы удалить пользовательский атрибут, передайте ключ `scheme_version` в опции `WithDropAttribute` метода `AlterTable`:

  ```go
  err = db.Table().Do(ctx,
    func(ctx context.Context, s table.Session) (err error) {
      return s.AlterTable(ctx, path.Join(db.Name(), "series"),
        options.WithDropAttribute("scheme_version"),
      )
    },
  )
  ```

- CLI

  Чтобы удалить пользовательский атрибут, передайте ключ `scheme_version` в опции `--attributes` команды [ydb table attribute drop](../reference/ydb-cli/table-attribute-drop.md):

  ```bash
  ydb table attribute drop --attributes scheme_version series
  ```

{% endlist %}
