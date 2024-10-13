# CPU bottleneck

High CPU usage can lead to slow query processing and increased response times. When CPU resources are constrained, the database may have difficulty handling complex queries or large transaction volumes.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/cpu-bottleneck.md) %}

## Recommendation

Add additional [database nodes](../../../../concepts/glossary.md#database-node) or allocate more CPU cores to the existing nodes.
