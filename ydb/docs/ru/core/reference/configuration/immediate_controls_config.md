# immediate_controls_config

Конфигурация `immediate_controls_config` — это набор динамических параметров для настройки компонентов {{ydb-short-name}}, таких как [DataShard](../../concepts/glossary.md#data-shard), [Coordinator](../../concepts/glossary.md#coordinator), [SchemeShard](../../concepts/glossary.md#scheme-shard), [BlobStorage](../../concepts/glossary.md#distributed-storage) и другие. Эти настройки позволяют адаптировать поведение кластера под специфичные сценарии, например, настроив пороги автоматического разделения шардов при росте данных или увеличения нагрузки. Изменение конфигурации выполняется через CMS, перезапуск кластера при этом не требуется.

## Синтаксис

```yaml
immediate_controls_config:
  ...
  scheme_shard_controls:
   force_shard_split_data_size: 2147483648
   ...
```

## Параметры

|Параметр|Минимальное значение|Максимальное значение|Значение по умолчанию|Описание|
|:---|:---|:---|:---|:---|
|`scheme_shard_controls.force_shard_split_data_size`|10 МиБ|16 ГиБ|2 ГиБ|Партиция таблицы будет разделяться при достижении заданного размера данных независимо от других [параметров партиционирования таблицы](../../concepts/datamodel/table.md#partitioning_row_table).|