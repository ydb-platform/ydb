# INTO RESULT

Lets you set a custom label for [SELECT](../select/index.md){% if feature_mapreduce and process_command == "PROCESS" %}, [PROCESS](../process.md), or [REDUCE](../reduce.md){% endif %}. It can't be used along with [DISCARD](../discard.md).

**Examples:**

```yql
SELECT 1 INTO RESULT foo;
```

```yql
SELECT * FROM
my_table
WHERE value % 2 == 0
INTO RESULT `Result name`;
```

