1. Open Grafana.

1. Compare the following Grafana charts:

    - **DB overview** > **Latency** > **Read only tx server latency**

        ![](../_assets/cpu-read-only-tx-latency.png)

    - **DataShard** > **RowRead rows**

        ![](../_assets/cpu-row-read-rows.png)

If the spikes on these charts align, the increased latencies may be related to the higher number of rows being read from the database. In this case, the available database nodes might not be sufficient to handle the increased load.