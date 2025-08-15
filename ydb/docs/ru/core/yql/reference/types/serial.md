
# Серийные типы данных

Серийные типы данных представляют собой целые числа, но с дополнительным механизмом генерации значений. Эти типы данных используются для создания автоинкрементных колонок, а именно: для каждой новой строки, добавляемой в таблицу, будет автоматически генерироваться уникальное значение для такой колонки (подобно типу [SERIAL](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL) в PostgreSQL или свойству [AUTO_INCREMENT](https://dev.mysql.com/doc/refman/9.0/en/example-auto-increment.html) в MySQL).

## Описание

При определении такого типа для колонки создаётся отдельный схемный объект `Sequence`, привязанный к этой колонке и являющийся генератором последовательности, из которого извлекаются значения. Этот объект является приватным и скрыт от пользователя. `Sequence` будет уничтожен вместе с таблицей.

Объект `Sequence` поддерживает ряд параметров, определяющих его поведение, которые можно изменить после создания `Sequence` с помощью команды [ALTER SEQUENCE](../syntax/alter-sequence.md).

По умолчанию генерируемые значения начинаются с единицы, увеличиваются на один при каждом новом значении и ограничены в соответствии с выбранным типом.

{% note info %}

Столбцы типа `Serial` поддерживаются как для колонок, входящих в состав первичного ключа, так и для неключевых колонок.

Однако такие колонки нельзя [изменить](../syntax/alter_table/family#mod-column-groups) или [удалить](../syntax/alter_table/columns.md) из таблицы — при попытке выполнить эти операции будет возвращена ошибка.

{% endnote %}

| Тип           | Максимальное значение | Тип значения |
|---------------|-----------------------|--------------|
| `SmallSerial` | $2^{15}–1$            | `Int16`      |
| `Serial2`     | $2^{15}–1$            | `Int16`      |
| `Serial`      | $2^{31}–1$            | `Int32`      |
| `Serial4`     | $2^{31}–1$            | `Int32`      |
| `Serial8`     | $2^{63}–1$            | `Int64`      |
|  `BigSerial`  | $2^{63}–1$            | `Int64`      |

При переполнении `Sequence` на вставке будет возвращаться ошибка:

```text
Error: Failed to get next val for sequence: /dev/test/users/_serial_column_user_id, status: SCHEME_ERROR
    <main>: Error: sequence [OwnerId: <some>, LocalPathId: <some>] doesn't have any more values available, code: 200503
```

{% note info %}

Cледующее значение выдаётся генератором до непосредственной вставки в таблицу и уже будет считаться использованным, даже если строка, содержащая это значение, не была успешно вставлена, например, при откате транзакции. Поэтому множество значений такой колонки может содержать пропуски и состоять из нескольких промежутков.

{% endnote %}

Для таблиц с автоинкрементными колонками поддержаны операции [copy](../../../reference/ydb-cli/tools-copy.md), [rename](../../../reference/ydb-cli/commands/tools/rename.md), [dump](../../../reference/ydb-cli/export-import/tools-dump.md), [restore](../../../reference/ydb-cli/export-import/import-file.md) и [import](../../../reference/ydb-cli/export-import/import-s3.md)/[export](../../../reference/ydb-cli/export-import/export-s3.md).

## Пример использования

Следует обратить внимание на правильный выбор колонок для [PRIMARY KEY](../../../dev/primary-key/row-oriented.md). Для масштабируемости нагрузки и высокой производительности стоит избегать записи строк с монотонно возрастающими первичными ключами. В этом случае все записи будут попадать в последнюю партицию, и вся нагрузка будет приходиться на один сервер.

Например, в качестве первого элемента ключа использовать можно хеш от всего первичного ключа либо его части, чтобы равномерно распределять данные по партициям кластера.

``` yql
CREATE TABLE users (
    user_hash Uint64,
    user_id Serial,
    name Utf8,
    email Utf8,
    PRIMARY KEY (user_hash, user_id)
);
```

Хеш для поля `user_hash` можно рассчитать на стороне приложения, например, используя хеш-функцию от `email`.

``` yql
UPSERT INTO users (user_hash, name, email) VALUES (123456789, 'Alice', 'alice@example.com');
INSERT INTO users (user_hash, name, email) VALUES (987654321, 'Bob', 'bob@example.com');
REPLACE INTO users (user_hash, name, email) VALUES (111111111, 'John', 'john@example.com');
```

``` yql
SELECT * FROM users;
```

Результат (значения `user_hash` приведены для примера):

| user_hash   | email                 | name  | user_id |
|-------------|-----------------------|-------|---------|
| 123456789   | `alice@example.com`   | Alice | 1       |
| 987654321   | `bob@example.com`     | Bob   | 2       |
| 111111111   | `john@example.com`    | John  | 3       |

Можно самостоятельно указать значение `Serial`-колонки при вставке, например для восстановления данных. В этом случае вставка будет выполняться, как с обычной целочисленной колонкой, и `Sequence` затрагиваться при таком запросе никак не будет:

``` yql
UPSERT INTO users (user_hash, user_id, name, email) VALUES (222222222, 10, 'Peter', 'peter@example.com');
```

### Пример неудачной схемы

``` yql
CREATE TABLE users_bad (
    user_id Serial,
    name Utf8,
    email Utf8,
    PRIMARY KEY (user_id)
);
```

В этом примере автоинкрементная колонка является единственным и первым элементом ключа — это приведёт к неравномерной нагрузке и узкому месту на последней партиции.

