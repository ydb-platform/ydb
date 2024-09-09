# Serial типы данных

Эти типы данных используются для создания автоинкрементных колонок, а именно для каждой новой строки, добавляемой в таблицу, будет автоматически генерироваться уникальное значение для такой колонки (подобно типу [SERIAL](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL) в PostgreSQL или свойству [AUTO_INCREMENT](https://dev.mysql.com/doc/refman/9.0/en/example-auto-increment.html) в MySQL)

## Пример использования

``` yql
CREATE TABLE users (
    user_id SERIAL,
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
alice@example.com | Alice | 1
bob@example.com | Bob | 2
John@example.com | John | 3

Можно самостоятельно указать значение Serial колонки при вставке, в этом случае вставка будет выполняться, как с обычной колонкой.
``` yql
UPSERT INTO users (user_id, name, email) VALUES (4, 'Peter', 'peter@example.com');
```
## Описание
Только ключевые колонки могут иметь тип `Serial`.

При определении такого типа для колонки создается отдельный схемный объект `Sequence`, привязанный к этой колонке, являющийся генератором последовательности, из которого извлекаются значения. Этот объект является приватным и скрыт от пользователя. `Sequence` будет уничтожен, когда будет уничтожена таблица.

Значения последовательности начинаются с единицы, выдаются с шагом равным единице и ограничены в зависимости от используемого типа. 

Тип | Ограничение | Тип значения
----- | ----- | -----
`SmallSerial` | 2<sup>15</sup>–1 | `Int16`
`Serial2` | 2<sup>15</sup>–1 | `Int16`
`Serial` | 2<sup>31</sup>–1 | `Int32`
`Serial4` | 2<sup>31</sup>–1 | `Int32`
`Serial8` | 2<sup>63</sup>–1 | `Int64`
`BigSerial` | 2<sup>63</sup>–1 | `Int64`

При переполнении `Sequence` на вставке будет возвращаться ошибка.

Отметим, что следующее значение выдается генератором до непосредственной вставки в таблицу и будет уже считаться использованным, даже если строка, содержащая данное значение, не была успешно вставлена, например, при откате транзакции. Поэтому множество значений такой колонки могут иметь “дыры” и может состоять из нескольких промежутков.

Для таблиц с автоинкрементными колонками поддержаны операции копирования и `backup`/`restore`.







