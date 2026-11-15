Distributes the rows of an ordered [partition](../../syntax/select/window.md#partition) into a specified number of groups. The groups are numbered starting with one. For each row, the `NTILE` function returns the number of the group to which the row belongs.

### Signature

```yql
NTILE(Uint64)->Uint64
```

### Examples
