# Настройка времени жизни строк (TTL) таблицы

В этом разделе приведены примеры настройки TTL строковых и колоночных таблиц при помощи {{ ydb-short-name }} SDK.

## Включение TTL для существующих строковых и колоночных таблиц {#enable-on-existent-table}

В приведенном ниже примере строки таблицы `mytable` будут удаляться спустя час после наступления времени, записанного в колонке `created_at`:

{% list tabs group=tool %}

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

Следующий пример демонстрирует использование колонки `modified_at` с числовым типом (`Uint32`) в качестве TTL-колонки. Значение колонки интерпретируется как секунды от Unix-эпохи:

{% list tabs group=tool %}

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

## Включение вытеснения во внешнее S3-совместимое хранилище {#enable-tiering-on-existing-tables}

{% include [OLTP_not_allow_note](../../_includes/not_allow_for_oltp_note.md) %}

Для включения вытеснения требуется объект [external data source](../../concepts/datamodel/external_data_source.md), описывающий подключение к внешнему хранилищу. Создание объекта external data source возможно через [YQL](../../yql/reference/recipes/ttl.md#enable-tiering-on-existing-tables) и {{ ydb-short-name }} CLI.

В следующем примере строки таблицы `mytable` будут переноситься в бакет, описанный во внешнем источнике данных `/Root/s3_cold_data`, спустя час после наступления времени, записанного в колонке `created_at`, а спустя 24 часа будут удаляться:

{% list tabs group=tool %}

{% if oss == true %}

- C++

  ```c++
  session.AlterTable(
      "mytable",
      TAlterTableSettings()
          .BeginAlterTtlSettings()
              .Set("created_at", {
                      TTtlTierSettings(TDuration::Hours(1), TTtlEvictToExternalStorageAction("/Root/s3_cold_data")),
                      TTtlTierSettings(TDuration::Hours(24), TTtlDeleteAction("/Root/s3_cold_data"))
                  })
          .EndAlterTtlSettings()
  );
  ```

{% endif %}

{% endlist %}

## Включение TTL для вновь создаваемой таблицы {#enable-for-new-table}

Для вновь создаваемой таблицы можно передать настройки TTL вместе с ее описанием:

{% list tabs group=tool %}

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

## Выключение TTL {#disable}

{% list tabs group=tool %}

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

## Получение настроек TTL {#describe}

Текущие настройки TTL можно получить из описания таблицы:

{% list tabs group=tool %}

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

