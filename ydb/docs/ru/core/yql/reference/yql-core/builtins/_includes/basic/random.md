## Random... {#random}

Генерирует псевдослучайное число:

* `Random()` — число с плавающей точкой (Double) от 0 до 1;
* `RandomNumber()` — целое число из всего диапазона Uint64;
* `RandomUuid()` — [Uuid version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

**Сигнатуры**
```
Random(T1[, T2, ...])->Double
RandomNumber(T1[, T2, ...])->Uint64
RandomUuid(T1[, T2, ...])->Uuid
```

При генерации случайных чисел аргументы не используются и нужны исключительно для управления моментом вызова. В каждый момент вызова возвращается новое случайное число. Поэтому:

{% if ydb_non_deterministic_functions %}
* Повторный вызов Random в рамках **одного запроса** при идентичном наборе аргументов не гарантирует получения одинаковых наборов случайных чисел. Значения будут равны, если вызовы Random попадут в одну фазу исполнения.
{% else %}
* Повторный вызов Random в рамках **одного запроса** при идентичном наборе аргументов возвращает тот же самый набор случайных чисел. Важно понимать, что речь именно про сами аргументы (текст между круглыми скобками), а не их значения.
{% endif %}
* Вызовы Random с одним и тем же набором аргументов в **разных запросах** вернут разные наборы случайных чисел.

{% note warning %}

Если Random используется в [именованных выражениях](../../../syntax/expressions.md#named-nodes), то его однократное вычисление не гарантируется. В зависимости от оптимизаторов и среды исполнения он может посчитаться как один раз, так и многократно. Для гарантированного однократного подсчета необходимо в этом случае материализовать именованное выражение в таблицу.

{% endnote %}

Сценарии использования:

* `SELECT RANDOM(1);` — получить одно случайное значение на весь запрос и несколько раз его использовать (чтобы получить несколько, можно передать разные константы любого типа);
* `SELECT RANDOM(1) FROM table;` — одно и то же случайное число на каждую строку таблицы;
* `SELECT RANDOM(1), RANDOM(2) FROM table;` — по два случайных числа на каждую строку таблицы, все числа в каждой из колонок одинаковые;
* `SELECT RANDOM(some_column) FROM table;` — разные случайные числа на каждую строку таблицы;
* `SELECT RANDOM(some_column), RANDOM(some_column) FROM table;` — разные случайные числа на каждую строку таблицы, но в рамках одной строки — два одинаковых числа;
* `SELECT RANDOM(some_column), RANDOM(some_column + 1) FROM table;` или `SELECT RANDOM(some_column), RANDOM(other_column) FROM table;` — две колонки, и все с разными числами.

**Примеры**
``` yql
SELECT
    Random(key) -- [0, 1)
FROM my_table;
```

``` yql
SELECT
    RandomNumber(key) -- [0, Max<Uint64>)
FROM my_table;
```

``` yql
SELECT
    RandomUuid(key) -- Uuid version 4
FROM my_table;
```

``` yql
SELECT
    RANDOM(column) AS rand1,
    RANDOM(column) AS rand2, -- same as rand1
    RANDOM(column, 1) AS randAnd1, -- different from rand1/2
    RANDOM(column, 2) AS randAnd2 -- different from randAnd1
FROM my_table;
```
