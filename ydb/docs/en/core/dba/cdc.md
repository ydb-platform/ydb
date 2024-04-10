# Change Data Capture

With [Change Data Capture](../concepts/cdc.md) (CDC), you can track changes in table data. {{ ydb-short-name }} provides access to changefeeds so that data consumers can monitor changes in near real time.

## Enabling and disabling CDC {#add-drop}

CDC is represented as a data schema object: a changefeed that can be added to a table or deleted from them using the [ADD CHANGEFEED and DROP CHANGEFEED](../yql/reference/syntax/alter_table.md#changefeed) directives of the YQL `ALTER TABLE` statement.

## Reading data from a topic {#read}

You can read data using an [SDK](../reference/ydb-sdk/index.md) or the [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md). As with any other data schema object, you can access a changefeed using its path that has the following format:

```txt
path/to/table/changefeed_name
```

> For example, if a table named `table` contains a changefeed named `updates_feed` in the `my` directory, its path looks as follows:
>
> ```text
> my/table/updates_feed
> ```

Before reading data, add a [consumer](../concepts/topic.md#consumer). Below is a sample command that adds a consumer named `my_consumer` to the `updates_feed` changefeed of the `table` table in the `my` directory:

```bash
{{ ydb-cli }} topic consumer add \
  my/table/updates_feed \
  --consumer=my_consumer
```

Next, you can use the created consumer to start tracking changes. Below is a sample command for tracking data changes in the CLI:

```bash
{{ ydb-cli }} topic read \
  my/table/updates_feed \
  --consumer=my_consumer \
  --format=newline-delimited \
  --wait
```

## Impact on table write performance {#performance-considerations}

When writing data to a table with CDC enabled, there are additional overheads for the following operations:

* Making records and saving them to a changefeed.
* Storing records in a changefeed.
* In some [modes](../yql/reference/syntax/alter_table.md#changefeed-options) (such as `OLD_IMAGE` and `NEW_AND_OLD_IMAGES`), data needs to be pre-fetched even if a user query doesn't require this.

As a result, queries may take longer to execute and size limits for stored data may be exceeded.

In real-world use cases, enabling CDC has virtually no impact on the query execution time (whatever the mode), since almost all data required for making records is stored in the cache , while the records themselves are sent to a topic asynchronously. However, record delivery background activity slightly (by 1% to 10%) increases CPU utilization.

When creating a changefeed for a table, the number of partitions of its storage (topic) is determined based on the current number of table partitions. If the number of source table partitions changes significantly (for example, after uploading a large amount of data or as a result of intensive accesses), an imbalance occurs between the table partitions and the topic partitions. This imbalance can also result in longer execution time for queries to modify data in the table or in unnecessary storage overheads for the changefeed. You can recreate the changefeed to correct the imbalance.

## Load testing {#workload}

As a load generator, you can use the feature of [emulating an online store](../reference/ydb-cli/commands/workload/stock) built into the {{ ydb-short-name }} CLI:

1. [Initialize](../reference/ydb-cli/commands/workload/stock#init) a test.
1. Add a changefeed:

   ```sql
   ALTER TABLE `orders` ADD CHANGEFEED `updates` WITH (
       FORMAT = 'JSON',
       MODE = 'UPDATES'
   );
   ```

1. Create a consumer:

   ```bash
   {{ ydb-cli }} topic consumer add \
     orders/updates \
     --consumer=my_consumer
   ```

1. Start tracking changes:

   ```bash
   {{ ydb-cli }} topic read \
     orders/updates \
     --consumer=my_consumer \
     --format=newline-delimited \
     --wait
   ```

1. [Generate](../reference/ydb-cli/commands/workload/stock#run) a load.

   The following changefeed appears in the CLI:

   ```text
   ...
   {"update":{"created":"2022-06-24T11:35:00.000000Z","customer":"Name366"},"key":[13195699997286404932]}
   {"update":{"created":"2022-06-24T11:35:00.000000Z","customer":"Name3894"},"key":[452209497351143909]}
   {"update":{"created":"2022-06-24T11:35:00.000000Z","customer":"Name7773"},"key":[2377978894183850258]}
   ...
   ```
