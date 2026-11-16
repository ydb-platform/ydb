{% note info %}

Using `--index-population-mode=Import` allows either using all available resources to restore indexes quickly, or limiting resources to copy index tables with minimal impact on the user workload. Import resources are limited by the [`queue_restore`](../../../configuration/resource_broker_config.md) queue of the resource broker.

{% endnote %}
