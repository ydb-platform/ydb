## INSERT INTO (вставка строк в таблицу){#insert}

Инструкция `INSERT INTO` предназначена для добавления строк в таблицу. Она может добавить одну или несколько строк за одно исполнение. Данные для добавления могут быть указаны явно в виде непосредственных значений или сформированы выражениями. Синтаксис конструкции `INSERT INTO` выглядит следующим образом:
```sql
INSERT INTO <название таблицы> (<название_столбца_таблицы>, ...)
VALUES (<значение_столбца_таблицы>, ...);
``` 

Пример добавления одного фильма (мы намеренно используем ненастоящие названия фильмов) в таблицу `movies`, которая была [создана ранее](#create_table): 
```sql
INSERT INTO movies (title, director, production_date, star, length) 
VALUES (
  'Space Battle: Episode X - No hope', 
  'George Lucas', 
  CAST('1977-05-25' AS DATE), 
  'Mark Hamill', 
  INTERVAL '2 hours 1 minute');
```

В этой записи мы не указали столбец `id` и не задали ему значение – это сделано намеренно, так как в таблице `movies` у столбца `id` задан тип данных `serial`. При выполнении инструкции `INSERT INTO` значение столбца `id` будет присвоено автоматически с учетом предыдущих значений – будет выполнен инкремент текущего значения `id`. 


С помощью `INSERT INTO` можно добавить сразу несколько строк в таблицу:
```sql
INSERT INTO movies (title, director, production_date, star, length) 
VALUES
('Indiana Johns and the Final Quest', 'Steven Spielberg', CAST('1989-05-24' AS DATE), 'Harrison Ford', INTERVAL '2 hours 7 minutes'),
('USA Wall Art', 'George Lucas', CAST('1973-08-01' AS DATE), 'Richard Dreyfuss', INTERVAL '1 hour 50 minutes'),
('THZ 1139', 'George Lucas', CAST('1971-03-11' AS DATE), 'Robert Duvall', INTERVAL '1 hour 26 minutes'),
('Space Battles: Episode One - The Ghostly Threat', 'George Lucas', CAST('1999-05-19' AS DATE), 'Liam Neeson', INTERVAL '2 hours 16 minutes'),
('Space Battles: Episode Two - Onslaught of the Duplicates', 'George Lucas', CAST('2002-05-16' AS DATE), 'Ewan McGregor', INTERVAL '2 hours 22 minutes'),
('Space Battles: Episode Three - Retaliation of the Sifth', 'George Lucas', CAST('2005-05-19' AS DATE), 'Hayden Christensen', INTERVAL '2 hours 20 minutes');
```

В обоих примерах для указания даты выпуска фильма, мы использовали функцию `CAST()`, которая используется для преобразования одного типа данных в другой. В данном случае мы с помощью ключевого слова `AS` и типа данных `DATE` явно указали, что хотим преобразовать строковое представление даты в формате [ISO8601](https://ru.wikipedia.org/wiki/ISO_8601) в тип данных `date`, который у нас заявлен у столбца `production_date`. 

Указать нужный тип данных, например, `DATE` можно и альтернативным способом с помощью оператора приведения типов `::`, который используется для явного приведения значения одного типа данных к другому. Пример использования оператора `::` может выглядеть так:
```sql
INSERT INTO movies (title, director, production_date, star, length) 
VALUES
('Indiana Johns and the Final Quest', 'Steven Spielberg', '1989-05-24'::date, 'Harrison Ford', INTERVAL '2 hours 7 minutes');
```

Помимо конструкции `INSERT INTO`, которая вставляет строки в таблицу, существует и её расширенная версия для выборки данных из другой таблицы и вставки их в новую (указанную) таблицу – `INSERT INTO ... SELECT ... FROM ...`. Последовательность столбцов и типы данных должны совпадать в конструкциях `INSERT INTO` и `SELECT`. 

Пример вставки нескольких фильмов из таблицы "movies" в таблицу "movies_2" (копия таблицы "movies"):
```sql
INSERT INTO movies_2 (title, director, production_date, star, length)
SELECT title, director, production_date, star, length
FROM movies
WHERE production_date < '1980-01-01'::date;
```
В этом примере из таблицы "movies" делается выборка фильмов произведенных до 1980 года и данные вставляются в таблицу "movies_2". В данном примере в обеих таблицах используются все столбцы, но выборка может быть сделана и по меньшему количеству столбцов. При вставки данных, можно не указывать перечень столбцов у `INSERT INTO`:
```sql
INSERT INTO movies_2
SELECT title, director, production_date, star
FROM movies
WHERE production_date < '1980-01-01'::date;
```