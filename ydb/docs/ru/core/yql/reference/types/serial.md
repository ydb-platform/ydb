
# Серийные типы данных

Серийные типы данных представляют собой целые числа, но с дополнительным механизмом генерации значений. Эти типы данных используются для создания автоинкрементных колонок, а именно для каждой новой строки, добавляемой в таблицу, будет автоматически генерироваться уникальное значение для такой колонки (подобно типу [SERIAL](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL) в PostgreSQL или свойству [AUTO_INCREMENT](https://dev.mysql.com/doc/refman/9.0/en/example-auto-increment.html) в MySQL).

## Пример использования

``` yql
CREATE TABLE users (
    user_id Serial,
    name Utf8,
    email Utf8,
    PRIMARY KEY (user_id)
);
```

``` yql
UPSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');
REPLACE INTO users (name, email) VALUES ('John', 'john@example.com');
```

``` yql
SELECT * FROM users;
```

email | name | user_id
----- | ----- | -----
`alice@example.com` | Alice | 1
`bob@example.com` | Bob | 2
`john@example.com` | John | 3

Можно самостоятельно указать значение `Serial` колонки при вставке, в этом случае вставка будет выполняться, как с обычной целочисленной колонкой, и `Sequence` затрагиваться при таком запросе никак не будет:

``` yql
UPSERT INTO users (user_id, name, email) VALUES (4, 'Peter', 'peter@example.com');
```

## Описание

Только колонки, участвующие в первичном ключе таблиц, могут иметь тип `Serial`.

При определении такого типа для колонки создаётся отдельный схемный объект `Sequence`, привязанный к этой колонке и являющийся генератором последовательности, из которого извлекаются значения. Этот объект является приватным и скрыт от пользователя. `Sequence` будет уничтожен вместе с таблицей.

Значения последовательности начинаются с единицы, выдаются с шагом, равным единице, и ограничены в зависимости от используемого типа.

Тип | Максимальное значение | Тип значения
----- | ----- | -----
`SmallSerial` | $2^{15}–1$ | `Int16`
`Serial2` | $2^{15}–1$ | `Int16`
`Serial` | $2^{31}–1$ | `Int32`
`Serial4` | $2^{31}–1$ | `Int32`
`Serial8` | $2^{63}–1$ | `Int64`
`BigSerial` | $2^{63}–1$ | `Int64`

При переполнении `Sequence` на вставке будет возвращаться ошибка:

```text
Error: Failed to get next val for sequence: /dev/test/users/_serial_column_user_id, status: SCHEME_ERROR
    <main>: Error: sequence [OwnerId: <some>, LocalPathId: <some>] doesn't have any more values available, code: 200503
```

Отметим, что следующее значение выдаётся генератором до непосредственной вставки в таблицу и уже будет считаться использованным, даже если строка, содержащая это значение, не была успешно вставлена, например, при откате транзакции. Поэтому множество значений такой колонки может содержать пропуски и состоять из нескольких промежутков.

Для таблиц с автоинкрементными колонками поддержаны операции [copy](../../../reference/ydb-cli/tools-copy.md), [dump](../../../reference/ydb-cli/export-import/tools-dump.md), [restore](../../../reference/ydb-cli/export-import/import-file.md) и [import](../../../reference/ydb-cli/export-import/import-s3.md)/[export](../../../reference/ydb-cli/export-import/export-s3.md).

