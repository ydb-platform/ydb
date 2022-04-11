## Named expressions {#named-nodes}

Complex queries may be sophisticated, containing lots of nested levels and/or repeating parts. In YQL, you can use named expressions to assign a name to an arbitrary expression or subquery. Named expressions can be referenced in other expressions or subqueries. In this case, the original expression/subquery is actually substituted at point of use.

A named expression is defined as follows:

```
<named-expr> = <expression> | <subquery>;
```

Here `<named-expr>` consists of a $ character and an arbitrary non-empty identifier (for example, `$foo`).

If the expression on the right is a tuple, you can automatically unpack it by specifying several named expressions separated by commas on the left:

```
<named-expr1>, <named-expr2>, <named-expr3> ... = <expression-returning-tuple>;
```

In this case, the number of expressions must match the tuple size.

Each named expression has a scope. It starts immediately after the definition of a named expression and ends at the end of the nearest enclosed namescope (for example, at the end of the query or at the end of the body of the [lambda function](#lambda), [ACTION](../../action.md#define-action){% if feature_subquery %}, [SUBQUERY](../../subquery.md#define-subquery){% endif %}{% if feature_mapreduce %}, or the cycle [EVALUATE FOR](../../action.md#evaluate-for){% endif %}).
Redefining a named expression with the same name hides the previous expression from the current scope.

If the named expression has never been used, a warning is issued. To avoid such a warning, use the underscore as the first character in the ID (for example, `$_foo`).
The named expression `$_` is called an anonymous named expression and is processed in a special way: it works as if `$_` would be automatically replaced by `$_<some_uniq_name>`.
Anonymous named expressions are convenient when you don't need the expression value. For example, to fetch the second element from a tuple of three elements, you can write:

```yql
$_, $second, $_ = AsTuple(1, 2, 3);
select $second;
```

An attempt to reference an anonymous named expression results in an error:

```yql
$_ = 1;
select $_; --- error: Unable to reference anonymous name $_
export $_; --- An error: Can not export anonymous name $_
```

{% if feature_mapreduce %}
Moreover, you can't import a named expression with an anonymous alias:

```yql
import utils symbols $sqrt as $_; --- error: Can not import anonymous name $_
```

{% endif %}
Anonymous argument names are also supported for [lambda functions](#lambda), [ACTION](../../action.md#define-action){% if feature_subquery %}, [SUBQUERY](../../subquery.md#define-subquery){% endif %}{% if feature_mapreduce %}, and in [EVALUATE FOR](../../action.md#evaluate-for){% endif %}.

{% note info %}

If named expression substitution results in completely identical subgraphs in the query execution graph, the graphs are combined to execute a subgraph only once.

{% endnote %}

**Examples**

```yql
$multiplier = 712;
SELECT
  a * $multiplier, -- $multiplier is 712
  b * $multiplier,
  (a + b) * $multiplier
FROM abc_table;
$multiplier = c;
SELECT
  a * $multiplier -- $multiplier is column c
FROM abc_table;
```

```yql
$intermediate = (
  SELECT
    value * value AS square,
    value
  FROM my_table
);
SELECT a.square * b.value
FROM $intermediate AS a
INNER JOIN $intermediate AS b
ON a.value == b.square;
```

```yql
$a, $_, $c = AsTuple(1, 5u, "test"); -- unpack a tuple
SELECT $a, $c;
```

```yql
$x, $y = AsTuple($y, $x); -- swap expression values
```

