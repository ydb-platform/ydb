# Configure row time-to-live (TTL) for a table

This section provides examples of configuring TTL for row and column tables using the {{ ydb-short-name }} SDK.

## Enable TTL for existing row and column tables {#enable-on-existent-table}

In the example below, rows of table `mytable` will be deleted one hour after the timestamp recorded in column `created_at`:

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

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Java


  ```java
  AlterTableSettings settings = new AlterTableSettings()
          .setTableTtl(TableTtl.dateTimeColumn("created_at", 3600));

  session.alterTable("mytable", settings).join().expectSuccess();
  ```

{% endlist %}

The following example demonstrates using column `modified_at` with a numeric type (`Uint32`) as the TTL column. The column value is interpreted as seconds since the Unix epoch:

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

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Java


  ```java
  AlterTableSettings settings = new AlterTableSettings()
          .setTableTtl(TableTtl.valueSinceUnixEpoch(
                  "modified_at",
                  TableTtl.TtlUnit.SECONDS,
                  3600
          ));

  session.alterTable("mytable", settings).join().expectSuccess();
  ```

{% endlist %}

## Enable eviction to external S3-compatible storage {#enable-tiering-on-existing-tables}

{% include [OLTP_not_allow_note](../../_includes/not_allow_for_oltp_note.md) %}

Enabling eviction requires an [external data source](../../concepts/datamodel/external_data_source.md) object that describes the connection to the external storage. Creating an external data source object is possible via [YQL](../../yql/reference/recipes/ttl.md#enable-tiering-on-existing-tables) and the {{ ydb-short-name }} CLI.

In the following example, rows of table `mytable` will be moved to the bucket described by external data source `/Root/s3_cold_data` one hour after the timestamp recorded in column `created_at`, and will be deleted after 24 hours:

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

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Go

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Python

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Java

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Enable TTL for a newly created table {#enable-for-new-table}

For a newly created table, you can pass TTL settings together with its description:

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

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Java

  TTL for a table is set in `TableDescription` at creation. You can check the settings via `describeTable`.


  ```java
  import tech.ydb.core.grpc.GrpcTransport;
  import tech.ydb.table.TableClient;
  import tech.ydb.table.description.TableDescription;
  import tech.ydb.table.description.TableTtl;
  import tech.ydb.table.session.SessionRetryContext;
  import tech.ydb.table.values.PrimitiveType;

  public class TtlCreateTableExample {

      private static final String TABLE_NAME = "mytable";

      public static void main(String[] args) {
          String connectionString = System.getenv().getOrDefault(
                  "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

          try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
               TableClient tableClient = TableClient.newClient(transport).build()) {

              SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
              String tablePath = transport.getDatabase() + "/" + TABLE_NAME;

              TableDescription description = TableDescription.newBuilder()
                      .addNullableColumn("id", PrimitiveType.Uint64)
                      .addNullableColumn("expire_at", PrimitiveType.Timestamp)
                      .setPrimaryKey("id")
                      .setTtlSettings(TableTtl.dateTimeColumn("expire_at", 0))
                      .build();

              retryCtx.supplyStatus(session -> session.createTable(tablePath, description))
                      .join().expectSuccess("create table failed");

              TableTtl ttl = retryCtx.supplyResult(session -> session.describeTable(tablePath))
                      .join().getValue().getTableDescription().getTableTtl();
              System.out.println("TTL column: " + ttl.getColumnName());
          }
      }
  }
  ```

{% endlist %}

## Disable TTL {#disable}

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

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Java


  ```java
  AlterTableSettings settings = new AlterTableSettings()
          .setTableTtl(TableTtl.notSet());

  session.alterTable("mytable", settings).join().expectSuccess();
  ```

{% endlist %}

## Retrieve TTL settings {#describe}

Current TTL settings can be obtained from the table description:

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

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
- Java


  ```java
  import tech.ydb.core.grpc.GrpcTransport;
  import tech.ydb.table.TableClient;
  import tech.ydb.table.description.TableTtl;
  import tech.ydb.table.session.SessionRetryContext;

  public class TtlDescribeExample {

      public static void main(String[] args) {
          String connectionString = System.getenv().getOrDefault(
                  "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

          try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
               TableClient tableClient = TableClient.newClient(transport).build()) {

              SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
              String tablePath = transport.getDatabase() + "/mytable";

              TableTtl ttl = retryCtx.supplyResult(session -> session.describeTable(tablePath))
                      .join().getValue().getTableDescription().getTableTtl();

              System.out.println("TTL enabled: " + ttl.isEnabled());
              System.out.println("TTL column: " + ttl.getColumnName());
          }
      }
  }
  ```

{% endlist %}
