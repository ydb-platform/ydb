## YQL::, s-expressions {#s-expressions}

For the full list of internal YQL functions, see the [documentation for s-expressions](/docs/s_expressions/functions), an alternative low-level YQL syntax. Any of the functions listed there can also be called from the SQL syntax by adding the `YQL::` prefix to its name. However, we don't recommend doing this, because this mechanism is primarily intended to temporarily bypass possible issues and for internal testing purposes.

If the function is available in SQL syntax without the `YQL::` prefix, then its behavior may differ from the same-name function from the s-expressions documentation, if any.

