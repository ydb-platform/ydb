1. Analyze CPU utilization in all pools:

    1. Open the **CPU** dashboard in Grafana.

    1. See if the following charts show any spikes:

        - **CPU by execution pool** chart

            ![](../_assets/cpu-by-pool.png)

        - **User pool - CPU by host** chart

            ![](../_assets/cpu-user-pool.png)

        - **System pool - CPU by host** chart

            ![](../_assets/cpu-system-pool.png)

        - **Batch pool - CPU by host** chart

            ![](../_assets/cpu-batch-pool.png)

        - **IC pool - CPU by host** chart

            ![](../_assets/cpu-ic-pool.png)

        - **IO pool - CPU by host** chart

            ![](../_assets/cpu-io-pool.png)

1. If the spike is in the user pool, analyze changes in the user load that might have caused the CPU bottleneck. See the following charts on the **DB overview** dashboard in Grafana:

    - **Requests** chart

        ![](../_assets/requests.png)

    - **Request size** chart

        ![](../_assets/request-size.png)

    - **Response size** chart

        ![](../_assets/response-size.png)

    Also, see all of the charts in the **Operations** section of the **DataShard** dashboard.

2. If the spike is in the batch pool, check if there are any backups running.
