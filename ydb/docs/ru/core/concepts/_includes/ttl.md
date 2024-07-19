# Time to Live (TTL)

В разделе описан принцип работы TTL, его ограничения, а также приведены примеры команд и фрагменты кода, с помощью которых можно включить, настроить и выключить TTL.

## Принцип работы {#how-it-works}

{{ ydb-short-name }} позволяет указать для строковой или колоночной таблицы колонку (TTL-колонка), значения которой будут использоваться для определения времени жизни строк. TTL автоматически удаляет из таблицы строки, когда проходит указанное количество секунд от времени, записанного в TTL-колонку.

{% note warning %}

Строка с `NULL` в TTL-колонке никогда не будет удалена.

{% endnote %}

Момент времени, когда строка таблицы может быть удалена, определяется по следующей формуле:

```text
expiration_time = valueof(ttl_column) + expire_after_seconds
```

{% note info %}

Не гарантируется, что удаление произойдет именно в `expiration_time` — оно может случиться позже. Если важно исключить из выборки логически устаревшие, но пока еще физически не удаленные строки, нужно использовать фильтрацию уровня запроса.

{% endnote %}

Непосредственно удалением данных занимается фоновая операция удаления — *Background Removal Operation* (*BRO*), состоящая из 2 стадий:

1. Проверка значений в TTL-колонке.
1. Удаление устаревших данных.

*BRO* обладает следующими свойствами:

* Единицей параллельности является [партиция таблицы](../datamodel/table.md#partitioning).
* Для таблиц со [вторичными индексами](../secondary_indexes.md) стадия удаления является [распределенной транзакцией](../transactions.md#distributed-tx).

## Гарантии {#guarantees}

* Для одной и той же партиции *BRO* запускается с периодичностью, заданной в настройках TTL. Интервал запуска по умолчанию — 1 час, минимально допустимое значение — 15 минут.
* Гарантируется согласованность данных. Значение в TTL-колонке повторно проверяется во время стадии удаления. Таким образом, если между стадиями 1 и 2 значение в TTL-колонке модифицируется (например, запросом `UPDATE`) и перестает соответствовать критерию удаления, такая строка не будет удалена.

## Ограничения {#restrictions}

* TTL-колонка должна быть одного из следующих типов:
  * `Date`;
  * `Datetime`;
  * `Timestamp`;
  * `Uint32`;
  * `Uint64`;
  * `DyNumber`.
* Значение TTL-колонки с числовым типом (`Uint32`, `Uint64`, `DyNumber`) интерпретируется как величина от [Unix-эпохи]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Unix_time){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Unix-время){% endif %}. Поддерживаемые единицы измерения (задаются в настройках TTL):
  * секунды;
  * миллисекунды;
  * микросекунды;
  * наносекунды.
* Нельзя указать несколько TTL-колонок.
* Нельзя удалить TTL-колонку. Если это все же требуется, сначала нужно [выключить TTL](#disable) на таблице.

## Настройка {#setting}

Управление настройками TTL в настоящий момент возможно с использованием:

* [YQL](../../yql/reference/index.md).
* [Консольного клиента {{ ydb-short-name }}](../../reference/ydb-cli/index.md).
* {{ ydb-short-name }} {% if oss %}C++, {% endif %}Go и Python [SDK](../../reference/ydb-sdk/index.md).

### Включение TTL для существующих строковых и колоночных таблиц {#enable-on-existent-table}

В приведенном ниже примере строки таблицы `mytable` будут удаляться спустя час после наступления времени, записанного в колонке `created_at`:

{% list tabs %}

- YQL

  ```yql
  ALTER TABLE `mytable` SET (TTL = Interval("PT1H") ON created_at);
  ```

- CLI

  ```bash
  $ {{ ydb-cli }} -e <endpoint> -d <database> table ttl set --column created_at --expire-after 3600 mytable
  ```

{% if oss == true %}

- C++

  ```c++
  session.AlterTable(
      "mytable",
      TAlterTableSettings()
          .BeginAlterTtlSettings()
              .Set("created_at", TDuration::Hours(1))
          .EndAlterTtlSettings()
  );
  ```

{% endif %}

- Go

  ```go
  err := session.AlterTable(ctx, "mytable",
    options.WithSetTimeToLiveSettings(
      options.NewTTLSettings().ColumnDateType("created_at").ExpireAfter(time.Hour),
    ),
  )
  ```

- Python

  ```python
  session.alter_table('mytable', set_ttl_settings=ydb.TtlSettings().with_date_type_column('created_at', 3600))
  ```

{% endlist %}

{% note tip %}

При настройке TTL с использованием YQL, `Interval` создается из строкового литерала в формате [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) с [некоторыми ограничениями](../../yql/reference/builtins/basic#data-type-literals).

{% endnote %}

Следующий пример демонстрирует использование колонки `modified_at` с числовым типом (`Uint32`) в качестве TTL-колонки. Значение колонки интерпретируется как секунды от Unix-эпохи:

{% list tabs %}

- YQL

  ```yql
  ALTER TABLE `mytable` SET (TTL = Interval("PT1H") ON modified_at AS SECONDS);
  ```

- CLI

  ```bash
  $ {{ ydb-cli }} -e <endpoint> -d <database> table ttl set --column modified_at --expire-after 3600 --unit seconds mytable
  ```

{% if oss == true %}

- C++

  ```c++
  session.AlterTable(
      "mytable",
      TAlterTableSettings()
          .BeginAlterTtlSettings()
              .Set("modified_at", TTtlSettings::EUnit::Seconds, TDuration::Hours(1))
          .EndAlterTtlSettings()
  );
  ```

{% endif %}

- Go

  ```go
  err := session.AlterTable(ctx, "mytable",
    options.WithSetTimeToLiveSettings(
      options.NewTTLSettings().ColumnSeconds("modified_at").ExpireAfter(time.Hour),
    ),
  )
  ```

- Python

  ```python
  session.alter_table('mytable', set_ttl_settings=ydb.TtlSettings().with_value_since_unix_epoch('modified_at', UNIT_SECONDS, 3600))
  ```

{% endlist %}

### Включение TTL для вновь создаваемой таблицы {#enable-for-new-table}

Для вновь создаваемой таблицы можно передать настройки TTL вместе с ее описанием:

{% list tabs %}

- YQL

  ```yql
  CREATE TABLE `mytable` (
      id Uint64,
      expire_at Timestamp,
      PRIMARY KEY (id)
  ) WITH (
      TTL = Interval("PT0S") ON expire_at
  );
  ```

{% if oss == true %}

- C++

  ```c++
  session.CreateTable(
      "mytable",
      TTableBuilder()
          .AddNullableColumn("id", EPrimitiveType::Uint64)
          .AddNullableColumn("expire_at", EPrimitiveType::Timestamp)
          .SetPrimaryKeyColumn("id")
          .SetTtlSettings("expire_at")
          .Build()
  );
  ```

{% endif %}

- Go

  ```go
  err := session.CreateTable(ctx, "mytable",
    options.WithColumn("id", types.Optional(types.TypeUint64)),
    options.WithColumn("expire_at", types.Optional(types.TypeTimestamp)),
    options.WithTimeToLiveSettings(
      options.NewTTLSettings().ColumnDateType("expire_at"),
    ),
  )
  ```

- Python

  ```python
  session.create_table(
      'mytable',
      ydb.TableDescription()
      .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
      .with_column(ydb.Column('expire_at', ydb.OptionalType(ydb.DataType.Timestamp)))
      .with_primary_key('id')
      .with_ttl(ydb.TtlSettings().with_date_type_column('expire_at'))
  )
  ```

{% endlist %}

### Выключение TTL {#disable}

{% list tabs %}

- YQL

  ```yql
  ALTER TABLE `mytable` RESET (TTL);
  ```

- CLI

  ```bash
  $ {{ ydb-cli }} -e <endpoint> -d <database> table ttl reset mytable
  ```

{% if oss == true %}

- C++

  ```c++
  session.AlterTable(
      "mytable",
      TAlterTableSettings()
          .BeginAlterTtlSettings()
              .Drop()
          .EndAlterTtlSettings()
  );
  ```

{% endif %}

- Go

  ```go
  err := session.AlterTable(ctx, "mytable",
    options.WithDropTimeToLive(),
  )
  ```

- Python

  ```python
  session.alter_table('mytable', drop_ttl_settings=True)
  ```

{% endlist %}

### Получение настроек TTL {#describe}

Текущие настройки TTL можно получить из описания таблицы:

{% list tabs %}

- CLI

  ```bash
  $ {{ ydb-cli }} -e <endpoint> -d <database> scheme describe mytable
  ```

{% if oss == true %}

- C++

  ```c++
  auto desc = session.DescribeTable("mytable").GetValueSync().GetTableDescription();
  auto ttl = desc.GetTtlSettings();
  ```

{% endif %}

- Go

  ```go
  desc, err := session.DescribeTable(ctx, "mytable")
  if err != nil {
    // process error
  }
  ttl := desc.TimeToLiveSettings
  ```

- Python

  ```python
  desc = session.describe_table('mytable')
  ttl = desc.ttl_settings
  ```

{% endlist %}
