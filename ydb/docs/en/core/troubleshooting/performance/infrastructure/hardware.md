# Hardware issues

Malfunctioning storage drives and network cards, until replaced, significantly impact database performance up to total unavailability of the affected server. CPU issues might lead to server failure and higher load on the remaining {{ ydb-short-name }} nodes.

## Diagnostics

Use the hardware monitoring tools that your operating system and data center provide to diagnose hardware issues.

You can also use the **Healthcheck** in [Embedded UI](../../../reference/embedded-ui/index.md) to diagnose some hardware issues:

- **Storage issues**

    1. On the **Storage** tab, select the **Degraded** filter to list storage groups or nodes that contain degraded or failed storage.

    1. Check for any degradation in the storage system performance on the **Distributed Storage Overview** and **PDisk Device single disk** dashboards in Grafana.

- **Network issues**

    Refer to [{#T}](network.md).

## Recommendations

Contact the responsible party for the affected hardware to resolve the underlying issue. If you are part of a larger organization, this could be an in-house team managing low-level infrastructure. Otherwise, contact the cloud service or hosting provider's support service.
