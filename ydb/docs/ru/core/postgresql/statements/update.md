# UPDATE (изменения строк таблицы)

{% include [../_includes/alert_preview.md)](../_includes/alert_preview.md) %}

Синтаксис инструкции `UPDATE`:
```sql
UPDATE <table name>
SET <column name> = [<new value>, CASE ... END]
WHERE <search column name> = [<search value>, IN]
```

Конструкция `UPDATE ... SET ... WHERE` работает так:
1. **Задаётся название таблицы** – `UPDATE`` `<table name>`, в которой будет произведено обновление данных;
2. **Указывается название столбца** –  `SET` `<column name>`, где следует обновить данные. Может применяться конструкция для указания набора данных `CASE ... END`;
3. **Задаётся новое значение** – `<new value>`;
4. **Указываются критерии поиска** –  `WHERE` с указанием колонки для поиска `<search column name>` и значения, которому должен соответствовать критерий поиска <search value>. Если применяется `CASE`, тогда указывается оператор `IN` с перечислением значений <column name>.

## Обновление одной строки в таблице с условиями

#|
|| **Обновление без условий** | **Обновление с условиями** ||
||
```sql
UPDATE people
SET name = 'Alexander'
WHERE lastname = 'Doe';
```
|
```sql
UPDATE people
SET age = 31
WHERE country = 'USA' AND city = 'Los Angeles';
```
||
|#

В примере "Обновление с условиями" используется оператор объединения условий `AND` (`И`) – условие будет выполнено только тогда, когда обе его части будут отвечать условиям истины. Также может использоваться оператор `OR`(`ИЛИ`) –  условие будет выполнено, если хотя бы одна из его частей будет отвечать условиям истины. Оператор и условий может быть множество:
```sql
...
WHERE country = 'USA' AND city = 'Los Angeles' OR city = 'Florida';
```

## Обновление одной записи в таблице с использованием выражений или функций {#update_set_func_where}
Часто при обновление данные нужно произвести с ними математические действия видоизменить с помощью функции.

#|
|| **Обновление с применением выражений** | **Обновление с применением функций** ||
||
```sql
UPDATE people
SET age = age + 1
WHERE country = 'Canada';
```
|
```sql
UPDATE people
SET name = UPPER(name)
WHERE country = 'USA';
```
||
|#


## Обновление нескольких полей строки таблице {#update_set_where}
Обновить данные можно в нескольких колонках одновременно. Для этого делается перечисление <column name> = <column new value> после ключевого слова `SET`:
```sql
UPDATE people
SET country = 'Russia', city = 'Moscow'
WHERE lastname = 'Smith';
```

## Обновление нескольких строк в таблицы с применением конструкции CASE ... END {#update_set_case_end_where}
Для одновременного обновления разных значений в разных строках можно использовать инструкцию `CASE ... END`, с вложенными условиями выборки данных `WHEN <column name> <condition> (=,>,<) THEN <new value>`. Далее следует конструкция `WHERE <column name> IN (<column value>, ...)`, которая позволяет задать список значений, по которым будет выполнено условие.

Пример, где изменяется возраст (`age`) людей (`people`) в зависимости от их имен:

```sql
UPDATE people
SET age = CASE
            WHEN name = 'John' THEN 32
            WHEN name = 'Jane' THEN 26
          END
WHERE name IN ('John', 'Jane');
```
{% include [../_includes/alert_locks.md](../_includes/alert_locks.md) %}