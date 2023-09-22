{% if feature_federated_queries %}

При работе с [внешними файловыми источниками данных](../../../../concepts/datamodel/external_data_source.md) можно дополнительно указывать ряд параметров:

{% include [s3_with](select/s3_with.md) %}

**Пример**
```sql
INSERT INTO `connection`.`test/`
WITH
(
  FORMAT = "csv_with_names"
)
SELECT
    "value" AS value, "name" AS name
```

Где:

* `connection` — название соединения с S3 ({{ objstorage-full-name }}).
* `test/`— путь внутри бакета, куда будут записаны данные. При записи создаются файлы со случайными именами.

{% endif %}
