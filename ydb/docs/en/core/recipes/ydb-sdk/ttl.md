# Configuring Time to Live (TTL)

This section contains recipes for configuration of table's TTL with {{ ydb-short-name }} SDK.

## Enabling TTL for an existing table {#enable-on-existent-table}

In the example below, the items of the `mytable` table will be deleted an hour after the time set in the `created_at` column:

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

The example below shows how to use the `modified_at` column with a numeric type (`Uint32`) as a TTL column. The column value is interpreted as the number of seconds since the Unix epoch:

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

## Enabling data eviction to S3-compatible external storage {#enable-tiering-on-existing-tables}

{% include [OLTP_not_allow_note](../../_includes/not_allow_for_oltp_note.md) %}

To enable data eviction, an [external data source](../../concepts/datamodel/external_data_source.md) object that describes a connection to the external storage is needed. Refer to [YQL recipe](../../yql/reference/recipes/ttl.md#enable-tiering-on-existing-tables) for examples of creating an external data source.

In the following example, rows of the table `mytable` will be moved to the bucket described in the external data source `/Root/s3_cold_data` one hour after the time recorded in the column `created_at` and will be deleted after 24 hours:

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

## Enabling TTL for a newly created table {#enable-for-new-table}

For a newly created table, you can pass TTL settings along with the table description:

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

## Disabling TTL {#disable}

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

## Getting TTL settings {#describe}

The current TTL settings can be obtained from the table description:

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

