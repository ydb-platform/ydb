# INSERT INTO

Adds rows to the table.

The table is searched by name in the database specified by the [USE](use.md) operator.

`INSERT INTO` lets you perform the following operations:

* Adding constant values using [`VALUES`](values.md).

  ```yql
  INSERT INTO my_table (Key1, Key2, Value1, Value2)
  VALUES (345987,'ydb', 'Pied piper', 1414);
  COMMIT;
  ```

  ```yql
  INSERT INTO my_table (key, value)
  VALUES ("foo", 1), ("bar", 2);
  ```

* Saving the `SELECT` result.

  ```yql
  INSERT INTO my_table
  SELECT Key AS Key1, "Empty" AS Key2, Value AS Value1
  FROM my_table1;
  ```

Inserts can be made with one or more modifiers. A modifier is specified after the `WITH` keyword following the table name: `INSERT INTO ... WITH SOME_HINT`.
If a modifier has a value, it's indicated after the `=` sign: `INSERT INTO ... WITH SOME_HINT=value`.
If necessary, specify multiple modifiers, they should be enclosed in parentheses: `INSERT INTO ... WITH (SOME_HINT1=value, SOME_HINT2, SOME_HINT3=value)`.

