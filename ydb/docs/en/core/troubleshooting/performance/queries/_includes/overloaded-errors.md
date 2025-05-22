1. Open the **[DB overview](../../../../reference/observability/metrics/grafana-dashboards.md#dboverview)** Grafana dashboard.

1. In the **API details** section, see if the **Soft errors (retriable)** chart shows any spikes in the rate of queries with the `OVERLOADED` status.

    ![](../_assets/soft-errors.png)

1. To check if the spikes in overloaded errors were caused by exceeding the limit of 15000 queries in table partition queues:

    1. In the [Embedded UI](../../../../reference/embedded-ui/index.md), go to the **Databases** tab and click on the database.

    1. On the **Navigation** tab, ensure the required database is selected.

    1. Open the **Diagnostics** tab.

    1. Open the **Top shards** tab.

    1. In the **Immediate** and **Historical** tabs, sort the shards by the **InFlightTxCount** column and see if the top values reach the 15000 limit.

1. To check if the spikes in overloaded errors were caused by tablet splits and merges, see [{#T}](../../schemas/splits-merges.md).

1. To check if the spikes in overloaded errors were caused by exceeding the 1000 limit of open sessions, in the Grafana **[DB status](../../../../reference/observability/metrics/grafana-dashboards.md#dbstatus)** dashboard, see the **Session count by host** chart.

1. See the [overloaded shards](../../schemas/overloaded-shards.md) issue.
