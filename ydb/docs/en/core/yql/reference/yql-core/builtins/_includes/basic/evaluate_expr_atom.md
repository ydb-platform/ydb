## EvaluateExpr, EvaluateAtom {#evaluate_expr_atom}

Evaluate an expression before the start of the main calculation and input its result to the query as a literal (constant). In many contexts, where only a constant would be expected in standard SQL (for example, in table names, in the number of rows in [LIMIT](../../../syntax/select/limit_offset.md), and so on), this functionality is implicitly enabled automatically.

EvaluateExpr can be used where the grammar already expects an expression. For example, you can use it to:

* Round the current time to days, weeks, or months and insert it into the query to ensure correct [query caching](../../../syntax/pragma.md#yt.querycachemode), although usually when [functions are used to get the current time](#currentutcdate), query caching is completely disabled.
* Run a heavy calculation with a small result once per query instead of once per job.

EvaluateAtom lets you dynamically create an [atom](../../../types/special.md), but since atoms are mainly controlled from a lower [s-expressions](/docs/s_expressions/functions) level, it's generally not recommended to use this function directly.

The only argument for both functions is the expression for calculation and substitution.

Restrictions:

* The expression must not trigger MapReduce operations.
* This functionality is fully locked in YQL over YDB.

**Examples:**

```yql
$now = CurrentUtcDate();
SELECT EvaluateExpr(
    DateTime::MakeDate(DateTime::StartOfWeek($now)
    )
);
```

