1. Analyze CPU utilization in all pools:

    1. Open the **CPU** dashboard in Grafana.

    1. See if the following charts show any spikes:

        - **CPU by execution pool** chart
        - **User pool - CPU by host** chart
        - **System pool - CPU by host** chart
        - **Batch pool - CPU by host** chart
        - **IC pool - CPU by host** chart
        - **IO pool - CPU by host** chart

1. Analyze changes in the user load that might have caused the CPU bottleneck. See the following charts on the **DB overview** in Grafana:

    - **Requests** chart

    - **Request size** chart

    - **Response size** chart

    Also, see all of the charts in the **Operations** section of the **DataShard** dashboard. These charts show the number of rows processed per query.

1. Contact your DBA and inquire about {{ ydb-short-name }} backups.
