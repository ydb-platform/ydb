# immediate_controls_config

The `immediate_controls_config` section provides a set of dynamic parameters for fine-tuning {{ydb-short-name}} components, including [DataShard](../../concepts/glossary.md#data-shard), [Coordinator](../../concepts/glossary.md#coordinator), [SchemeShard](../../concepts/glossary.md#scheme-shard), [BlobStorage](../../concepts/glossary.md#distributed-storage), and others. These settings let you adapt cluster behavior to specific scenarios — for example, by configuring thresholds for automatic shard splitting as data grows or load increases.

## Syntax

```yaml
immediate_controls_config:
  ...
  scheme_shard_controls:
    force_shard_split_data_size: 2147483648
    ...
```

## Parameters

|Parameter|Minimum value|Maximum value|Default value|Description|
|:---|:---|:---|:---|:---|
|`scheme_shard_controls.force_shard_split_data_size`|10 MiB|16 GiB|2 GiB|A table partition is forcibly split when it reaches the specified data size, even if the table's [partition size threshold or maximum partition count](../../concepts/datamodel/table.md#partitioning_row_table) would otherwise prevent the split. Specify the value in bytes.|
