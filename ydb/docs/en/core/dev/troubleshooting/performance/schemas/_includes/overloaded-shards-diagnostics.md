# Diagnostics of overloaded shards

1. Analyze the **Overloaded shard count** chart in the **DB overview** Grafana dashboard.

    ![](../_assets/overloaded-shards-dashboard.png)

    The chart shows whether the {{ ydb-short-name }} cluster has overloaded shards, but it does not indicate which specific table shards are overloaded.

1. To identify the table with an overloaded shard, follow these steps:

    1. In the [Embedded UI](../../../../../reference/embedded-ui/index.md), go to the **Databases** tab and click the database.

    1. On the **Navigation** tab, ensure the entire database is selected.

    1. Open the **Diagnostics** tab.

    1. Open the **Top shards** tab.

    1. In the **Immediate** and **Historical** tabs, sort the shards by the **CPUCores** column and analyze the information.

    ![](../_assets/partitions-by-cpu.png)

1. To pinpoint the schema issue, follow these steps:

    1. Retrieve information about the problematic table using the [{{ ydb-short-name }} CLI](../../../../../reference/ydb-cli/index.md). Run the following command:

        ```bash
        ydb scheme describe <table_name>
        ```

    1. In the command output, analyze the **Auto partitioning settings**:

        * `Partitioning by size`
        * `Partitioning by load`
        * `Max partitions count`

        If the table does not have these options, see [Recommendations for table configuration](../overloaded-shards.md#table-config)

1. Analyze whether primary key values autoincrement monotonically.

    If they do, see [Recommendations for the imbalanced primary key](../overloaded-shards.md#pk-recommendations)