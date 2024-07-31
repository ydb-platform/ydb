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
* [CREATE TABLE](create_table.md)
* [DROP TABLE](drop_table.md)
* [INSERT](insert_into.md)
{% if feature_map_tables %}
* [UPDATE](update.md)
* [DELETE](delete.md)
{% endif %}
{% if feature_replace %}
* [REPLACE](replace_into.md)
{% endif %}
{% if feature_upsert %}
* [UPSERT](upsert_into.md)
{% endif %}
* [GROUP BY](group_by.md)
{% if feature_join %}
* [JOIN](join.md)
{% endif %}
{% if feature_window_functions %}
* [WINDOW](window.md)
{% endif %}
* [FLATTEN](flatten.md)
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
