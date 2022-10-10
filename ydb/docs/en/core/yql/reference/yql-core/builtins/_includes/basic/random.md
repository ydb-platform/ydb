## Random... {#random}

Generates a pseudorandom number:

* `Random()`: A floating point number (Double) from 0 to 1.
* `RandomNumber()`: An integer from the complete Uint64 range.
* `RandomUuid()`: [Uuid version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

**Signatures**
```
Random(T1[, T2, ...])->Double
RandomNumber(T1[, T2, ...])->Uint64
RandomUuid(T1[, T2, ...])->Uuid
```

No arguments are used for random number generation: they are only needed to control the time of the call. A new random number is returned at each call. Therefore:

{% if ydb_non_deterministic_functions %}
* If Random is called again within a **same query** and with a same set of arguments does not guarantee getting the same sets of random numbers. The values will be equal if the Random calls fall into the same execution phase.
{% else %}
* If Random is called again within a **same query** and with a same set of arguments, the same set of random numbers is returned. Keep in mind that we mean the arguments themselves (i.e., the text between parentheses) rather than their values.
{% endif %}
* Calling of Random with the same set of arguments in **different queries** returns different sets of random numbers.

{% note warning %}

If Random is used in [named expressions](../../../syntax/expressions.md#named-nodes), its one-time calculation is not guaranteed. Depending on the optimizers and runtime environment, it can be counted both once and multiple times. To make sure it's only counted once, materialize a named expression into a table.

{% endnote %}

Use cases:

* `SELECT RANDOM(1);`: Get one random value for the entire query and use it multiple times (to get multiple random values, you can pass various constants of any type).
* `SELECT RANDOM(1) FROM table;`: The same random number for each row in the table.
* `SELECT RANDOM(1), RANDOM(2) FROM table;`: Two random numbers for each row of the table, all the numbers in each of the columns are the same.
* `SELECT RANDOM(some_column) FROM table;`: Different random numbers for each row in the table.
* `SELECT RANDOM(some_column), RANDOM(some_column) FROM table;`: Different random numbers for each row of the table, but two identical numbers within the same row.
* `SELECT RANDOM(some_column), RANDOM(some_column + 1) FROM table;` or `SELECT RANDOM(some_column), RANDOM(other_column) FROM table;`: Two columns, with different numbers in both.

**Examples**
```yql
SELECT
    Random(key) -- [0, 1)
FROM my_table;
```

```yql
SELECT
    RandomNumber(key) -- [0, Max<Uint64>)
FROM my_table;
```

```yql
SELECT
    RandomUuid(key) -- Uuid version 4
FROM my_table;
```

```yql
SELECT
    RANDOM(column) AS rand1,
    RANDOM(column) AS rand2, -- same as rand1
    RANDOM(column, 1) AS randAnd1, -- different from rand1/2
    RANDOM(column, 2) AS randAnd2 -- different from randAnd1
FROM my_table;
```
