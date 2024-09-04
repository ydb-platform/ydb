# INTO RESULT

Позволяет задать пользовательскую метку для [SELECT](../select/index.md){% if feature_mapreduce and process_command == "PROCESS" %}, [PROCESS](../process.md) или [REDUCE](../reduce.md){% endif %}.{% if backend_name != "YDB" %} Не может быть задано одновременно с [DISCARD](../discard.md).{% endif %}

**Примеры:**

``` yql
SELECT 1 INTO RESULT foo;
```

``` yql
SELECT * FROM
my_table
WHERE value % 2 == 0
INTO RESULT `Название результата`;
```
