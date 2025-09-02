# INSERT INTO

Adds rows to the table.

The table is searched by name in the database specified by the [USE](use.md) operator.

`INSERT INTO` lets you perform the following operations:

* Adding constant values using [`VALUES`](values.md).

  ```yql
  INSERT INTO my_table (Column1, Column2, Column3, Column4)
  VALUES (345987,'ydb', 'Pied piper', 1414);
  COMMIT;
  ```

  ```yql
  INSERT INTO my_table (Column1, Column2)
  VALUES ("foo", 1), ("bar", 2);
  ```

* Saving the `SELECT` result.

  ```yql
  INSERT INTO my_table
  SELECT SourceTableColumn1 AS MyTableColumn1, "Empty" AS MyTableColumn2, SourceTableColumn2 AS MyTableColumn3
  FROM source_table;
  ```

## Using modifiers

Inserts can be made with one or more modifiers. A modifier is specified after the `WITH` keyword following the table name: `INSERT INTO ... WITH SOME_HINT`.

The following rules apply when working with modifiers:

- If a modifier has a value, it's indicated after the `=` sign: `INSERT INTO ... WITH SOME_HINT=value`.
- If necessary, specify multiple modifiers, they should be enclosed in parentheses: `INSERT INTO ... WITH (SOME_HINT1=value, SOME_HINT2, SOME_HINT3=value)`.

