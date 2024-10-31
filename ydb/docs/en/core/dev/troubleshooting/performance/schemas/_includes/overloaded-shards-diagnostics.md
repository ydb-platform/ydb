1. Analyze the **Overloaded shard count** chart in the **DB overview** Grafana dashboard.

    ![](../_assets/overloaded-shards-dashboard.png)

    The chart indicates whether the {{ ydb-short-name }} cluster has overloaded shards, but it does not specify which table's shards are overloaded.

2. To identify the table with overloaded shards, follow these steps:

    1. In the [Embedded UI](../../../../../reference/embedded-ui/index.md), go to the **Databases** tab and click on the database.

    2. On the **Navigation** tab, ensure the required database is selected.

    3. Open the **Diagnostics** tab.

    4. Open the **Top shards** tab.

    5. In the **Immediate** and **Historical** tabs, sort the shards by the **CPUCores** column and analyze the information.

    ![](../_assets/partitions-by-cpu.png)

    Additionally, the information about overloaded shards is provided as a system table. For more information, see [{#T}](../../../../system-views.md#top-overload-partitions).

    {% endnote %}

3. To pinpoint the schema issue, follow these steps:

    1. Retrieve information about the problematic table using the [{{ ydb-short-name }} CLI](../../../../../reference/ydb-cli/index.md). Run the following command:

        ```bash
        ydb scheme describe <table_name>
        ```

    2. In the command output, analyze the **Auto partitioning settings**:

        * `Partitioning by size`
        * `Partitioning by load`
        * `Max partitions count`

        If the table does not have these options, see [Recommendations for table configuration](../overloaded-shards.md#table-config).

4. Analyze whether primary key values increment monotonically:

    - Check the data type of the primary key column. `Serial` data types are used for autoincrementing values.

    - Check the application logic.

    - Calculate the difference between the minimum and maximum values of the primary key column. Then compare this value to the number of rows in a given table. If these values match, the primary key might be incrementing monotonically.

    If primary key values do increase monotonically, see [Recommendations for the imbalanced primary key](../overloaded-shards.md#pk-recommendations).
