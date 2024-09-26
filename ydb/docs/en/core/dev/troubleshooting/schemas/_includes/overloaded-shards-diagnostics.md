# Diagnostics of overloaded shards

1. Analyze the **Overloaded shard count** chart in the **DB overview** Grafana dashboard.

    ![](../_assets/overloaded-shards-dashboard.png)

    The chart displays if the {{ ydb-short-name }} cluster has overloaded shards, but it does not show which table shards are overloaded.

1. To identify the table that has an overloaded shard, follow these steps:

    1. In the [Embedded UI](../../../../reference/embedded-ui/index.md), on the **Databases** tab, click the database.

    1. In the **Navigation** tab, ensure that the entire database is selected.

    1. Open the **Diagnostics** tab.

    1. Open the **Top shards** tab.

    1. On the **Immediate** and **Historical** tabs, sort the shards by the **CPUCores** column and analyze the information.

    ![](../_assets/partitions-by-cpu.png)

1. To pinpoint the schema issue, follow these steps:

    1. Get information about the problem table using [YDB CLI](../../../../reference/ydb-cli/index.md). Run the following command:

        ```bash
        ydb scheme describe <table_name>
        ```

    1. In the command output, analyze the **Auto partitioning settings**:

        * `Partitioning by size`
        * `Partitioning by load`
        * `Max partitions count`