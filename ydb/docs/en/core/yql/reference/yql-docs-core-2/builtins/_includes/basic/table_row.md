## TableRow{% if feature_join %}, JoinTableRow{% endif %} {#tablerow}

Getting the entire table row as a structure. No arguments{% if feature_join %}. `JoinTableRow` in case of `JOIN` always returns a structure with table prefixes{% endif %}.

**Example**

```yql
SELECT TableRow() FROM my_table;
```

