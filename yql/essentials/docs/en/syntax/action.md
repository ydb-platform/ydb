# ACTION

## DEFINE ACTION {#define-action}

Specifies a named action that is a parameterizable block of multiple top-level expressions.

### Syntax

1. `DEFINE ACTION`: action definition.
1. [Action name](expressions.md#named-nodes) that will be used to access the defined action further in the query.
1. The values of parameter names are listed in parentheses.
1. `AS` keyword.
1. List of top-level expressions.
1. `END DEFINE`: The marker of the last expression inside the action.

One or more of the last parameters can be marked with a question mark `?` as optional. If they are omitted during the call, they will be assigned the `NULL` value.

## DO {#do}

Executes an `ACTION` with the specified parameters.

### Syntax

1. `DO`: Executing an action.
1. The named expression for which the action is defined.
1. The values to be used as parameters are listed in parentheses.

`EMPTY_ACTION`: An action that does nothing.

{% note info %}

In large queries, you can use separate files for action definition and include them to the main query using [EXPORT](export_import.md#export) + [IMPORT](export_import.md#import) so that instead of one long text you can have several logical parts that are easier to navigate. An important nuance: the `USE my_cluster;` directive in the import query does not affect behavior of actions declared in other files.

{% endnote %}

### Example

```yql
DEFINE ACTION $hello_world($name, $suffix?) AS
    $name = $name ?? ($suffix ?? "world");
    SELECT "Hello, " || $name || "!";
END DEFINE;

DO EMPTY_ACTION();
DO $hello_world(NULL);
DO $hello_world("John");
DO $hello_world(NULL, "Earth");
```

## BEGIN .. END DO {#begin}

Performing an action without declaring it (anonymous action).

### Syntax

1. `BEGIN`.
1. List of top-level expressions.
1. `END DO`.

An anonymous action can't include any parameters.

### Example

```yql
DO BEGIN
    SELECT 1;
    SELECT 2 -- here and in the previous example, you might omit ';' before END
END DO
```

## EVALUATE IF {#evaluate-if}

`EVALUATE IF`: Executing an action depending on the condition. It's followed by:

1. Condition.
2. [DO](#do) with the name and parameters of the action or an anonymous action.
3. An optional `ELSE` followed by the second `DO` for a situation where the condition is not met.

## EVALUATE FOR {#evaluate-for}

`EVALUATE FOR`: Executing an action for each item in the list. It's followed by:

1. [A named expression](expressions.md#named-nodes) applied to each next element in the list.
2. `IN` keyword.
3. The above-declared named expression applied to the list the action is executed on.
4. [DO](#do) with the name and parameters of an action or an anonymous action. In the parameters, you can use both the current element from the first paragraph and any named expressions declared above, including the list itself.
5. An optional `ELSE` followed by the second `DO` for the situation when the list is empty.

### Examples

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

Note that `EVALUATE` is run before the operation starts.

{% endnote %}
