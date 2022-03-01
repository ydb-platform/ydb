## SessionStart {#session-start}

No arguments. It's allowed only if there is [SessionWindow](../../../syntax/group_by.md#session-window) in [GROUP BY](../../../syntax/group_by.md) / [PARTITION BY](../../../syntax/window.md#partition).
Returns the value of the `SessionWindow` key column. If `SessionWindow` has two arguments, it returns the minimum value of the first argument within the group/section.
In the case of the expanded version `SessionWindow`, it returns the value of the second element from the tuple returned by `<calculate_lambda>`, for which the first tuple element is `True`.

