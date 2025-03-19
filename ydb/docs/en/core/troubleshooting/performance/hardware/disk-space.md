# Disk space

A lack of available disk space can prevent the database from storing new data, resulting in the database becoming read-only. This can also cause slowdowns as the system tries to reclaim disk space by compacting existing data more aggressively.

## Diagnostics

1. See if the **[DB overview > Storage](../../../reference/observability/metrics/grafana-dashboards.md#dboverview)** charts in Grafana show any spikes.

1. In [Embedded UI](../../../reference/embedded-ui/index.md), on the **Storage** tab, analyze the list of available storage groups and nodes and their disk usage.

    {% note tip %}

    Use the **Out of Space** filter to list only the storage groups with full disks.

    {% endnote %}

    ![](_assets/storage-groups-disk-space.png)

{% note info %}

It is also recommended to use the [Healthcheck API](../../../reference/ydb-sdk/health-check-api.md) to get this information.

{% endnote %}

## Recommendations

Add more [storage groups](../../../concepts/glossary.md#storage-group) to the database.

If the cluster doesn't have spare storage groups, configure them first. Add additional [storage nodes](../../../concepts/glossary.md#storage-node), if necessary.
