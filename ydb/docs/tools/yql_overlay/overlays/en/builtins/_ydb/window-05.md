Access a value from a row specified by position in the window's `ORDER BY` order within [window frame](../../syntax/select/window.md#frame). Arguments - the expression to access and the row number, starting with 1.

Optionally, the `IGNORE NULLS` modifier can be specified before `OVER`, which causes rows with `NULL` in the first argument's value to be skipped. The antonym of this modifier is `RESPECT NULLS`, which is the default behavior and may be skipped.

### Signature

```yql
NTH_VALUE(T,N)->T?
```

### Examples
