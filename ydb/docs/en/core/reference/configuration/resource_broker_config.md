# resource_broker_config

The resource broker is an [actor service](../../concepts/glossary.md#actor-service) that controls resource consumption on {{ ydb-short-name }} [nodes](../../concepts/glossary.md#node), such as:

- `CPU` — number of threads
- `Memory` — RAM

Different types of activities (background operations, [TTL](../../concepts/ttl.md) data deletion, etc.) run in different resource broker *queues*. Each queue has a limited number of resources:

| Queue name                | CPU | Memory | Description                                         |
|---------------------------| --- | --- |----------------------------------------------------|
| `queue_ttl`               | 2 | — | [TTL](../../concepts/ttl.md) data deletion operations.                |
| `queue_backup`            | 2 | — | [Backup](../../devops/backup-and-recovery.md#s3) operations.                |
| `queue_restore`           | 2 | — | [Restore from backup](../../devops/backup-and-recovery.md#s3) operations.     |
| `queue_build_index`       | 10 | — | [Online secondary index creation](../../concepts/secondary_indexes.md#index-add) operations.   |
| `queue_cdc_initial_scan` | 4 | — | [Initial table scan](../../concepts/cdc.md#initial-scan) operations.             |

{% note info %}

It is recommended to **extend** the resource broker configuration using [tags](../../devops/configuration-management/configuration-v2/dynamic-config-selectors.md#additional-yaml-tags) `!inherit` and `!append`.

{% endnote %}

Example of extending the resource broker configuration with a custom limit for the `queue_ttl` queue:

```yaml
resource_broker_config: !inherit
  queues: !append
  - name: queue_ttl
    limit:
      cpu: 4
```