## EVALUATE IF {#evaluate-if}

`EVALUATE IF`: Executing an action depending on the condition. It's followed by:

1. Condition.
2. [DO](#do) with the name and parameters of the action or an anonymous action.
3. An optional `ELSE` followed by the second `DO` for a situation where the condition is not met.

## EVALUATE FOR {#evaluate-for}

`EVALUATE FOR`: Executing an action for each item in the list. It's followed by:

1. [A named expression](../../expressions.md#named-nodes) applied to each next element in the list.
2. `IN` keyword.
3. The above-declared named expression applied to the list the action is executed on.
4. [DO](#do) with the name and parameters of an action or an anonymous action. In the parameters, you can use both the current element from the first paragraph and any named expressions declared above, including the list itself.
5. An optional `ELSE` followed by the second `DO` for the situation when the list is empty.

**Examples**

```yql
DEFINE ACTION $hello() AS
    SELECT "Hello!";
END DEFINE;

DEFINE ACTION $bye() AS
    SELECT "Bye!";
END DEFINE;

EVALUATE IF RANDOM(0) > 0.5
    DO $hello()
ELSE
    DO $bye();

EVALUATE IF RANDOM(0) > 0.1 DO BEGIN
    SELECT "Hello!";
END DO;

EVALUATE FOR $i IN AsList(1, 2, 3) DO BEGIN
    SELECT $i;
END DO;
```

```yql
-- copy the $input table to $count of new tables
$count = 3;
$input = "my_input";
$inputs = ListReplicate($input, $count);
$outputs = ListMap(
    ListFromRange(0, $count),
    ($i) -> {
        RETURN "tmp/out_" || CAST($i as String)
    }
);
$pairs = ListZip($inputs, $outputs);

DEFINE ACTION $copy_table($pair) as
    $input = $pair.0;
    $output = $pair.1;
    INSERT INTO $output WITH TRUNCATE
    SELECT * FROM $input;
END DEFINE;

EVALUATE FOR $pair IN $pairs
    DO $copy_table($pair)
ELSE
    DO EMPTY_ACTION (); -- you may omit this ELSE,
                        -- do nothing is implied by default
```

{% note info %}

Note that `EVALUATE` is run before the operation starts. Please also note that in `EVALUATE` you can't use [anonymous tables](../../select.md#temporary-tables).

{% endnote %}

