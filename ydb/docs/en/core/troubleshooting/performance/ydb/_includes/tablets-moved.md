1. See if the **Tablets moved by Hive** chart in the **[DB status](../../../../reference/observability/metrics/grafana-dashboards.md#dbstatus)** Grafana dashboard shows any spikes.

    ![](../_assets/tablets-moved.png)

        This chart displays the time-series data for the number of tablets moved per second.

1. See the Hive balancer stats.

    1. Open [Embedded UI](../../../../reference/embedded-ui/index.md).

    1. Click **Developer UI** in the upper right corner of the Embedded UI.

    1. In the **Developer UI**, navigate to **Tablets > Hive > App**.

        See the balancer stats in the upper right corner.

        ![cpu balancer](../_assets/cpu-balancer.jpg)

    1. Additionally, to see the recently moved tablets, click the **Balancer** button.

        The **Balancer** window will appear. The list of recently moved tablets is displayed in the **Latest tablet moves** section.
