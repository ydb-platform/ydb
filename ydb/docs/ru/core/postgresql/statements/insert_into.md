# INSERT INTO (вставка строк в таблицу)

{% include [../_includes/alert_preview.md)](../_includes/alert_preview.md) %}

Синтаксис инструкции `INSERT INTO`:
```sql
INSERT INTO <table name> (<column name>, ...)
VALUES (<value>);
```

Инструкция `INSERT INTO` предназначена для добавления строк в таблицу. Она может добавить одну или несколько строк за одно исполнение. Пример вставки одной строки в таблицу "people":
```sql
INSERT INTO people (name, lastname, age, country, state, city, birthday, sex)
VALUES ('John', 'Doe', 30, 'USA', 'California', 'Los Angeles', CAST('1992-01-15' AS Date), 'Male');
```

В этой записи мы не указали столбец `id` и не задали ему значение – это сделано намеренно, так как в таблице "people" у столбца "id" задан тип данных `Serial`. При выполнении инструкции `INSERT INTO` значение столбца "id" будет присвоено автоматически с учетом предыдущих значений – будет выполнен инкремент текущего значения "id".

Для множественной вставки строк в таблицу используется та же конструкция с перечислением групп данных для вставки через запятую:
```sql
INSERT INTO people (name, lastname, age, country, state, city, birthday, sex)
VALUES
    ('Jane', 'Smith', 25, 'Canada', 'Ontario', 'Toronto', CAST('1997-08-23' AS Date), 'Female'),
    ('Alice', 'Johnson', 28, 'UK', 'England', 'London', CAST('1994-05-05' AS Date), 'Female'),
    ('Bob', 'Brown', 40, 'USA', 'Texas', 'Dallas', CAST('1982-12-10' AS Date), 'Male'),
    ('Charlie', 'Davis', 35, 'Canada', 'Quebec', 'Montreal', CAST('1987-02-17' AS Date), 'Male'),
    ('Eve', 'Martin', 29, 'UK', 'Scotland', 'Edinburgh', CAST('1993-11-21' AS Date), 'Female'),
    ('Frank', 'White', 45, 'USA', 'Florida', 'Miami', CAST('1977-03-14' AS Date), 'Male'),
    ('Grace', 'Clark', 50, 'Canada', 'British Columbia', 'Vancouver', CAST('1972-04-26' AS Date), 'Female'),
    ('Hank', 'Miller', 33, 'UK', 'Wales', 'Cardiff', CAST('1989-07-30' AS Date), 'Male'),
    ('Ivy', 'Garcia', 31, 'USA', 'Arizona', 'Phoenix', CAST('1991-09-05' AS Date), 'Female'),
    ('Jack', 'Anderson', 22, 'Canada', 'Manitoba', 'Winnipeg', CAST('2000-06-13' AS Date), 'Male'),
    ('Kara', 'Thompson', 19, 'UK', 'Northern Ireland', 'Belfast', CAST('2003-10-18' AS Date), 'Female'),
    ('Liam', 'Martinez', 55, 'USA', 'New York', 'New York City', CAST('1967-01-29' AS Date), 'Male'),
    ('Molly', 'Robinson', 40, 'Canada', 'Alberta', 'Calgary', CAST('1982-12-01' AS Date), 'Female'),
    ('Noah', 'Lee', 47, 'UK', 'England', 'Liverpool', CAST('1975-05-20' AS Date), 'Male'),
    ('Olivia', 'Gonzalez', 38, 'USA', 'Illinois', 'Chicago', CAST('1984-03-22' AS Date), 'Female'),
    ('Paul', 'Harris', 23, 'Canada', 'Saskatchewan', 'Saskatoon', CAST('1999-08-19' AS Date), 'Male'),
    ('Quinn', 'Lewis', 34, 'UK', 'England', 'Manchester', CAST('1988-07-25' AS DATE), 'Female'),
    ('Rachel', 'Young', 42, 'USA', 'Ohio', 'Cleveland', CAST('1980-02-03' AS Date), 'Female');
```

В обоих примерах для указания даты выпуска фильма, мы использовали функцию `CAST()`, которая используется для преобразования одного типа данных в другой. В данном случае мы с помощью ключевого слова `AS` и типа данных `Date` явно указали, что хотим преобразовать строковое представление даты в формате [ISO8601](https://ru.wikipedia.org/wiki/ISO_8601).

Указать нужный тип данных, например, `DATE` можно и альтернативным способом с помощью оператора приведения типов `::`, который используется для явного приведения значения одного типа данных к другому. Пример использования оператора `::` может выглядеть так:
```sql
INSERT INTO people (name, lastname, age, country, state, city, birthday, sex)
VALUES ('Sam', 'Walker', 60, 'Canada', 'Nova Scotia', 'Halifax', '1962-04-15'::Date, 'Male');
```

{% include [../_includes/alert_locks.md](../_includes/alert_locks.md) %}