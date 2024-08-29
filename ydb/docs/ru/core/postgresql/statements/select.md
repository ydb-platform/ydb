# SELECT (Чтение строк из таблицы)

{% include [../_includes/alert_preview.md)](../_includes/alert_preview.md) %}

Синтаксис инструкции `SELECT`:
```sql
SELECT [<table column>, ... | *]
FROM [<table name> | <sub query>] AS <table name alias>
LEFT | RIGHT | CROSS | INNER JOIN <another table> AS <table name alias> ON <join condition>
WHERE <condition>
GROUP BY <table column>
HAVING <condition>
UNION | UNION ALL | EXCEPT | INTERSECT
ORDER BY <table column> [ASC | DESC]
LIMIT [<limit value>]
OFFSET <offset number>
```


## Вызов SELECT без указания целевой таблицы {#select_func}
`SELECT` используется для возврата вычислений на клиентскую сторону, в случае если он вызван без дополнительных конструкций, так как `FROM ...`, `INSERT INTO ...` и т.д. Например, `SELECT` можно использовать для работы с датами, преобразования чисел или подсчета длины строки:
```sql
SELECT CURRENT_DATE + INTERVAL '1 day';  -- Возвращает завтрашнюю дату
SELECT LENGTH('Hello');  -- Возвращает длину строки 'Hello'
SELECT CAST('123' AS INTEGER);  -- Преобразует строки в числа
```

Такое применение `SELECT` бывает полезно при тестировании, отладки выражений или SQL-функций без обращения к реальной таблице, но чаще `SELECT` используется для получения строк из одной или множества таблиц.


## Выборка значений из одного или нескольких столбцов {#select_from}
Для возвращения значений из одного или нескольких столбцов таблицы применяется `SELECT` в следующем виде:
```sql
SELECT <column name> , <column name>
FROM <table name>;
```

Чтобы прочитать все данные из таблицы, например, таблицы `people` – нужно выполнить команду `SELECT * FROM people;`, где  `*` – это оператор выбора данных по всем столбцам. При такой записи будут возвращены все строки из таблицы с данными по всем столбцам.

Вывести столбцы "id", "name" и "lastname" для всех строк таблицы `people` можно так:
```sql
SELECT id, name, lastname
FROM people;
```


## Ограничение получаемых результатов выборки с помощью WHERE {#select_from_where}
Для выборки только части строк - используется оператор `WHERE` с условиями выборки: `WHERE <column name> <condition> <column value>;`:
```sql
SELECT id, name, lastname
FROM people
WHERE age > 30;
```

`WHERE` позволяет использовать несколько операторов условного выбора (`AND` (И), `OR`(ИЛИ)) для создания сложных условий выборок, например, диапазонов:
```sql
SELECT id, name, lastname
FROM people
WHERE age > 30 AND age < 45;
```


## Получение части строк по условиям LIMIT и OFFSET {#select_from_where_limit}
Для ограничения количества строк в результатах выборки используется `LIMIT` с указанием количества строк:
```sql
SELECT id, name, lastname
FROM people
WHERE age > 30 AND age < 45
LIMIT 5;
```

Так на печать будет выведено 5 первых строк из выборки. С `OFFSET` можно указать сколько нужно пропустить строк, прежде чем начать выдавать строки на печать:
```sql
SELECT id, name, lastname
FROM people
WHERE age > 30 AND age < 45
OFFSET 3
LIMIT 5;
```

При указании `OFFSET 3` первые 3 строки результирующей выборки из таблицы `people` будут пропущены.


## Сортировка результатов выборки с помощью ORDER BY {#select_from_where_order_by}
По умолчанию база данных не гарантирует порядок возврата строк, и он может отличаться от запроса к запросу. Если требуется сохранить определенный порядок строк – используется инструкция `ORDER BY` с указанием столбца для сортировки и направлением сортировки:
```sql
SELECT id, name, lastname, age
FROM people
WHERE age > 30 AND age < 45
ORDER BY age DESC;
```
Сортировка происходит по результатам, которые возвращает (`SELECT`), а не по исходным столбцам таблицы (`FROM`). Сортировать можно в прямом порядке – `ASC` (от меньшего к большему - вариант по умолчанию, можно не указывать) и в обратном – `DESC` (от большего к меньшему). Как сортировка будет выполняться, зависит от типа данных столбца. Например, строки хранятся в utf-8 и сравниваются по "unicode collate" (по кодам символов).


## Группировка результатов выборки из одной или нескольких таблиц с помощью GROUP BY {#select_from_where_group_by}
`GROUP BY` используется для сбора данных по нескольким записям и группировки результатов по одному или нескольким столбцам. Синтаксис использования `GROUP BY`:
```sql
SELECT <column name>, <column name>, ...
FROM <table name>
[WHERE <column name> = <value>]
GROUP BY <column name>, <column name>, ...;
[HAVING <column name> = <limit column value>]
[LIMIT <value>]
[OFFSET <value>]
```
Пример группировки данных из таблицы "people" по полу ("sex") и возрасту ("age") с ограничением выборки (`WHERE`) по возрасту:
```sql
SELECT sex, age
FROM people
WHERE age > 40
GROUP BY sex, age;
```

В предыдущем примере мы использовали `WHERE` – необязательный параметр фильтрации результата, который фильтрует отдельные строки до применения `GROUP BY`. В следующем примере мы используем `HAVING` для исключения из результата строки групп, не удовлетворяющих условию. `HAVING` фильтрует строки групп, созданных `GROUP BY`. При использовании `HAVING` запрос превращается в группируемый, даже если `GROUP BY` отсутствует. Все выбранные строки считаются формирующими одну группу, а в списке SELECT и предложении HAVING можно обращаться к столбцам таблицы только из агрегатных функций. Такой запрос будет выдавать единственную строку, если результат условия HAVING — true, и ноль строк в противном случае.

**Примеры использования `HAVING`**:

#|
|| **`HAVING` + `GROUP BY`** | **`HAVING` + `WHERE` + `GROUP BY`** ||
||
```sql
SELECT sex, country, age
FROM people
GROUP BY sex, country, age
HAVING sex = 'Female';
```
|
```sql
SELECT sex, name,age
FROM people
WHERE age > 40
GROUP BY sex,name,age
HAVING sex = 'Female';
```
||
|#


## Объединение таблиц с помощью оператора JOIN {#select_from_join_on}
`SELECT` можно применять к нескольким таблицам с указанием типа соединения таблиц. Объединение таблиц задается через оператор `JOIN`, который бывает пяти типов: `LEFT JOIN`, `RIGHT JOIN`, `INNER JOIN`, `CROSS JOIN`, `FULL JOIN`. Когда выполняется `JOIN` по определенному условию, например, по ключу, и в одной из таблиц есть несколько строк с одинаковым значением этого ключа, получается [декартово произведение](https://ru.wikipedia.org/wiki/Прямое_произведение). Это означает, что каждая строка из одной таблицы будет соединена с каждой соответствующей строкой из другой таблицы.

### Объединение таблиц с помощью LEFT JOIN, RIGHT JOIN или INNER JOIN {#select_from_left_right__inner_join_on}
Синтаксис `SELECT` с использованием `LEFT JOIN`, `RIGHT JOIN`, `INNER JOIN`, `FULL JOIN` одинаков:
```sql
SELECT <table name left>.<column name>, ... ,
FROM <table name left>
LEFT | RIGHT | INNER | FULL JOIN <table name right> AS <table name right alias>
ON <table name left>.<column name> = <table name right>.<column name>;
```

Все операторы `JOIN`, кроме `CROSS JOIN` использую для присоединения таблиц ключевое слово `ON`. В случае `CROSS JOIN`, синтаксис его использования будет следующем: `CROSS JOIN <table name> AS <table name alias>;`. Рассмотрим пример использования каждого оператора `JOIN` в отдельности.

**LEFT JOIN** (или LEFT OUTER JOIN)
Возвращает все строки из левой таблицы и соответствующие строки из правой таблицы. Если нет совпадений, возвращает `NULL` (в выводе будет пустота) для всех колонок правой таблицы. Пример использования `LEFT JOIN`:
```sql
SELECT people.name, people.lastname, card.social_card_number
FROM people
LEFT JOIN social_card AS card
ON people.name = card.card_holder_name AND people.lastname = card.card_holder_lastname;
```

Результат выполнения SQL запроса с использованием `LEFT JOIN` без одной записи в правой таблице `social_card`:
```
 name   | lastname | social_card_number
---------+----------+--------------------
 John    | Doe      |          123456789
 Jane    | Smith    |          223456789
 Alice   | Johnson  |          323456789
 Bob     | Brown    |          423456789
 Charlie | Davis    |          523456789
 Eve     | Martin   |          623456789
 Frank   | White    |
```

**RIGHT JOIN** (или RIGHT OUTER JOIN)
Возвращает все строки из правой таблицы и соответствующие строки из левой таблицы. Если не существует совпадений, возвращает `NULL` для всех колонок левой таблицы. Этот тип `JOIN` редко используется, так как его функциональность можно заменить `LEFT JOIN`, меняя местами таблицы. Пример использования `RIGHT JOIN`:
```sql
SELECT people.name, people.lastname, card.social_card_number
FROM people
RIGHT JOIN social_card AS card
ON people.name = card.card_holder_name AND people.lastname = card.card_holder_lastname;
```

Результат выполнения SQL запроса с использованием `RIGHT JOIN` без одной записи в левой таблице `people`:
```
 name   | lastname | social_card_number
---------+----------+--------------------
John    | Doe      |          123456789
Jane    | Smith    |          223456789
Alice   | Johnson  |          323456789
Bob     | Brown    |          423456789
Charlie | Davis    |          523456789
Eve     | Martin   |          623456789
        |          |          723456789
```

**INNER JOIN** (или просто JOIN)
Возвращает строки, когда есть соответствующие значения в обеих таблицах. Исключает из результатов те строки, для которых нет совпадений в соединяемых таблицах. Пример использования `INNER JOIN`:
```sql
SELECT people.name, people.lastname, card.social_card_number
FROM people
RIGHT JOIN social_card AS card
ON people.name = card.card_holder_name AND people.lastname = card.card_holder_lastname;
```

Такой SQL запрос вернет только те строки, для которых есть совпадения в обеих таблицах:
```
 name   | lastname | social_card_number
---------+----------+--------------------
John    | Doe      |          123456789
Jane    | Smith    |          223456789
Alice   | Johnson  |          323456789
Bob     | Brown    |          423456789
Charlie | Davis    |          523456789
Eve     | Martin   |          623456789
```

**CROSS JOIN**
Возвращает комбинированный результат каждой строки левой таблицы с каждой строкой правой таблицы. Обычно `CROSS JOIN` используется, когда необходимо получить все возможные комбинации строк из двух таблиц. `CROSS JOIN` просто комбинирует каждую строку одной таблицы с каждой строкой другой без какого-либо условия, поэтому в его синтаксисе отсутствуют ключевые слова `ON` или: `CROSS JOIN <table name> AS <table name alias>;`.

Пример использования `CROSS JOIN` с ограничением вывода результата `LIMIT 5`:
```sql
SELECT people.name, people.lastname, card.social_card_number
FROM people
CROSS JOIN social_card AS card
LIMIT 5;
```

Пример выше вернет все возможные комбинации столбцов, участвующих в выборке из двух таблиц:
```
name | lastname | social_card_number
------+----------+--------------------
 John | Doe      |          123456789
 John | Doe      |          223456789
 John | Doe      |          323456789
 John | Doe      |          423456789
 John | Doe      |          523456789
```

**FULL JOIN** (или FULL OUTER JOIN)
Возвращает как совпавшие, так и не совпавшие строки в обеих таблицах, при этом возвращает `NULL` в колонках из таблицы, для которой не найдено совпадение. Пример выполнения SQL запроса с использованием `FULL JOIN`:
```sql
SELECT people.name, people.lastname, card.social_card_number
FROM people
FULL JOIN social_card AS card
ON people.name = card.card_holder_name AND people.lastname = card.card_holder_lastname;
```

В результате выполнения SQL запроса будет возвращен следующей вывод:
```
 name   | lastname | social_card_number
---------+----------+--------------------
 Liam    | Martinez |         1323456789
 Eve     | Martin   |          623456789
 Hank    | Miller   |          923456789
 Molly   | Robinson |         1423456789
 Sam     | Walker   |
 Paul    | Harris   |         1723456789
 Kara    | Thompson |         1223456789
         |          |         1923456789
...
```