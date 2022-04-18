{% if feature_insert_with_truncate %}

Inserts can be made with one or more modifiers. A modifier is specified after the `WITH` keyword following the table name: `INSERT INTO ... WITH SOME_HINT`.
If a modifier has a value, it's indicated after the `=` sign: `INSERT INTO ... WITH SOME_HINT=value`.
If necessary, specify multiple modifiers, they should be enclosed in parentheses: `INSERT INTO ... WITH (SOME_HINT1=value, SOME_HINT2, SOME_HINT3=value)`.

To clear the table of existing data before writing new data to it, add the modifier: `INSERT INTO ... WITH TRUNCATE`.

**Examples:**

```yql
INSERT INTO my_table WITH TRUNCATE
SELECT key FROM my_table_source;
```

{% endif %}

