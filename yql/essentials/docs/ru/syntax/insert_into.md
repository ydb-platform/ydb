
# INSERT INTO

Добавляет строки в таблицу.

Таблица ищется по имени в базе данных, заданной оператором [USE](use.md).

`INSERT INTO` позволяет выполнять следующие операции:

* Добавлять константные значения с помощью [`VALUES`](values.md).

  ```yql
  INSERT INTO my_table (Column1, Column2, Column3, Column4)
  VALUES (345987,'ydb', 'Яблочный край', 1414);
  COMMIT;
  ```

  ```yql
  INSERT INTO my_table (Column1, Column2)
  VALUES ("foo", 1), ("bar", 2);
  ```

* Сохранять результаты выборки `SELECT`.

  ```yql
  INSERT INTO my_table
  SELECT SourceTableColumn1 AS MyTableColumn1, "Empty" AS MyTableColumn2, SourceTableColumn2 AS MyTableColumn3
  FROM source_table;
  ```

## Использование модификаторов

Запись может выполняться с одним или несколькими модификаторами. Модификатор указывается после ключевого слова `WITH` после имени таблицы: `INSERT INTO ... WITH SOME_HINT`.

Действуют следующие правила:
- Если у модификатора есть значение, то оно указывается после знака `=`: `INSERT INTO ... WITH SOME_HINT=value`.
- Если необходимо указать несколько модификаторов, то они заключаются в круглые скобки: `INSERT INTO ... WITH (SOME_HINT1=value, SOME_HINT2, SOME_HINT3=value)`.
