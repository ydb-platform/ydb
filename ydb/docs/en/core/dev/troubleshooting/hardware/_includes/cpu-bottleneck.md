1. Open Grafana.

1. Compare the following Grafana charts:

    - **DB overview** > **Latency** > **Read only tx server latency**

        ![](../_assets/cpu-read-only-tx-latency.png)

    - **DataShard** > **RowRead rows**

        ![](../_assets/cpu-row-read-rows.png)

If the spikes on these charts coincide, increased latencies might have to do with the increased number of rows read from the database. That is the available database nodes do not cope with the increased load.