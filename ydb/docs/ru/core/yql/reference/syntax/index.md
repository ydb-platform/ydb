# Список статей по синтаксису YQL

* [Лексическая структура](lexer.md)
* [Выражения](expressions.md)

{% if feature_mapreduce %}

* [USE](use.md)

{% endif %}

* [SELECT](select/index.md)
* [VALUES](values.md)

{% if select_command == "SELECT STREAM" %}

* [SELECT STREAM](select_stream.md)

{% endif %}

* [CREATE TABLE](create_table/index.md)
* [DROP TABLE](drop_table.md)
* [TRUNCATE TABLE](truncate-table.md)
* [INSERT](insert_into.md)
* [SHOW CREATE](show_create.md)

{% if feature_map_tables %}

* [ALTER TABLE](alter_table/index.md)
* [UPDATE](update.md)
* [DELETE](delete.md)

{% endif %}

{% if feature_replace %}

* [REPLACE](replace_into.md)

{% endif %}

{% if feature_upsert %}

* [UPSERT](upsert_into.md)

{% endif %}

{% if feature_batch_operations %}

* [BATCH UPDATE](batch-update.md)
* [BATCH DELETE](batch-delete.md)

{% endif %}

* [GROUP BY](select/group-by.md)

{% if feature_join %}

* [JOIN](select/join.md)

{% endif %}

{% if feature_window_functions %}

* [WINDOW](select/window.md)

{% endif %}

* [FLATTEN](select/flatten.md)
* [ACTION](action.md)

{% if feature_mapreduce and process_command == "PROCESS" %}

* [SUBQUERY](subquery.md)

{% endif %}

{% if backend_name != "YDB" %}

* [DISCARD](discard.md)

{% endif %}

* [INTO RESULT](into_result.md)

{% if feature_mapreduce %}

{% if process_command == "PROCESS" %}

* [PROCESS](process.md)

{% endif %}

{% if process_command == "PROCESS STREAM" %}

* [PROCESS STREAM](process.md)

{% endif %}

{% if reduce_command == "REDUCE" %}

* [REDUCE](reduce.md)

{% endif %}

{% endif %}

* [PRAGMA](pragma.md)
* [DECLARE](declare.md)

{% if feature_mapreduce %}

* [EXPORT и IMPORT](export_import.md)

{% endif %}

{% if feature_topic_control_plane %}

* [CREATE TOPIC](create-topic.md)
* [ALTER TOPIC](alter-topic.md)
* [DROP TOPIC](drop-topic.md)

{% endif %}

{% if feature_async_replication %}

* [CREATE ASYNC REPLICATION](create-async-replication.md)
* [ALTER ASYNC REPLICATION](alter-async-replication.md)
* [DROP ASYNC REPLICATION](drop-async-replication.md)

{% endif %}

{% if feature_backup_collections %}

* [CREATE BACKUP COLLECTION](create-backup-collection.md)
* [BACKUP](backup.md)
* [RESTORE](restore-backup-collection.md)
* [DROP BACKUP COLLECTION](drop-backup-collection.md)

{% endif %}

{% if feature_transfer %}

* [CREATE TRANSFER](create-transfer.md)
* [ALTER TRANSFER](alter-transfer.md)
* [DROP TRANSFER](drop-transfer.md)

{% endif %}

* [COMMIT](commit.md)
* [CREATE VIEW](create-view.md)
* [ALTER VIEW](alter-view.md)
* [DROP VIEW](drop-view.md)
* [CREATE EXTERNAL DATA SOURCE](create-external-data-source.md)
* [CREATE EXTERNAL TABLE](create-external-table.md)
* [DROP EXTERNAL DATA SOURCE](drop-external-data-source.md)
* [DROP EXTERNAL TABLE](drop-external-table.md)
* [CREATE OBJECT (TYPE SECRET)](create-object-type-secret.md)
* [CREATE OBJECT (TYPE SECRET_ACCESS)](create-object-type-secret-access.md)
* [DROP OBJECT (TYPE SECRET)](drop-object-type-secret.md)
* [DROP OBJECT (TYPE SECRET_ACCESS)](drop-object-type-secret-access.md)
* [UPSERT OBJECT (TYPE SECRET)](upsert-object-type-secret.md)
* [CREATE RESOURCE POOL](create-resource-pool.md)
* [ALTER RESOURCE POOL](alter-resource-pool.md)
* [DROP RESOURCE POOL](drop-resource-pool.md)
* [CREATE RESOURCE POOL CLASSIFIER](create-resource-pool-classifier.md)
* [ALTER RESOURCE POOL CLASSIFIER](alter-resource-pool-classifier.md)
* [DROP RESOURCE POOL CLASSIFIER](drop-resource-pool-classifier.md)
* [CREATE USER](create-user.md)
* [ALTER USER](alter-user.md)
* [DROP USER](drop-user.md)
* [CREATE GROUP](create-group.md)
* [ALTER GROUP](alter-group.md)
* [DROP GROUP](drop-group.md)
* [GRANT](grant.md)
* [REVOKE](revoke.md)
* [Неподдерживаемые конструкции](not_yet_supported.md)
