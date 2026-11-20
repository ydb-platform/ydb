# Mark join

A mark join answers `IN` or `EXISTS` for every left row. The left row stays. One extra column says `true`, `false`, or `null`.

A join key is either mark or regular:

- A mark key is a column of the `IN` test
- A regular key puts rows in a group. A regular key may also have the `null == null` rule.

A mark key cannot have the `null == null` rule. The join checks this and rejects it.

The right side is built into a hash table. The table key is the whole join key, mark keys and regular keys. `true` comes only from a lookup in this table.

The rest of this note is about the other two answers, `false` and `null`, for each left row.

## One mark column, no regular keys

```sql
SELECT x IN (SELECT y FROM t2) FROM t1;
```

While the hash table is built, the join sets two flags: `BuildNotEmpty_` when the right side is not empty, and `BuildHasNull_` when some `y` is null.

| `x` | right side | answer |
|---|---|---|
| `1` | `1`, null, `2` | `true` |
| `3` | `1`, null, `2` | `null` |
| null | `1`, null, `2` | `null` |
| `3` | `1`, `2` | `false` |
| null | empty | `false` |

The check is:

- look up the full key. A hit is `true`
- if there is no hit, look at `BuildHasNull_`. If it is set, the answer is `null`
- otherwise the answer is `false`

`BuildNotEmpty_` answers a left `null`. That value is not looked up. If the right side has any row, the answer is `null`. If it is empty, the answer is `false`.

#### Complexity
Lookup: O(1). A hit is one hash lookup. A miss checks two booleans.

#### Spilling
Spilling works as usual. `BuildNotEmpty_` and `BuildHasNull_` are two booleans. They are not written to disk.


## One mark column and regular keys

```sql
SELECT x IN (SELECT y FROM t2 WHERE t2.k = t1.k) FROM t1;
```

The right rows are stored once in the main hash table, under the full key `(k, y)`.

`NullRows` and `OtherRows` are two more hash tables. Their key is only the regular key `k`. They do not store the row again. A lookup of `k` returns row numbers in the main table:

- `NullRows` returns the numbers of rows with this `k` whose `y` is null
- `OtherRows` returns the numbers of rows with this `k` whose `y` is not null. This table is built only when the left side can contain null

In this case one returned row number is enough. `y` is not read again.

The check is:

- look up the full key `(k, x)` in the main hash table. A hit is `true`
- if there is no hit, look up `k` in `NullRows`. If it returns a row number, the answer is `null`
- otherwise the answer is `false`

A left `null` is not looked up in the main hash table. If `NullRows` or `OtherRows` returns a row number for `k`, the answer is `null`. If both return nothing, the answer is `false`.

Right side: `(1, 10)`, `(1, null)`, `(2, 20)`.

| left `(k, x)` | answer |
|---|---|
| `(1, 10)` | `true`, the full key matches |
| `(1, 30)` | `null`, `NullRows` returns a row number for `k = 1` |
| `(1, null)` | `null`, `NullRows` or `OtherRows` returns a row number for `k = 1` |
| `(2, 10)` | `false`, `NullRows` returns nothing for `k = 2` |
| `(3, null)` | `false`, both tables return nothing for `k = 3` |

If the mark column cannot be null on either side, the second index is not built. A miss is `false`. If only the right side can contain null, only `NullRows` is filled.

#### Complexity
Lookup: O(1). One lookup by the full key, and on a miss one lookup by the regular key. The second lookup stops at the first row with that key.

#### Spilling
Spilling works as usual, with one change. The bucket is chosen by the regular key. The hash table inside the bucket is built and looked up by the full key. `NullRows` and `OtherRows` are not spilled. They are built for the bucket that is in memory, and built again when a spilled bucket is read back.

## Several mark columns and regular keys

DuckDB does not support this case. A correlated `IN` with several columns fails with `Correlated IN/ANY/ALL with multiple columns not yet supported`. The other cases in this note are supported.

```sql
SELECT (a, b) IN (SELECT c, d FROM t2 WHERE t2.k = t1.k) FROM t1;
```

The right rows are stored once in the main hash table, under the full key `(k, c, d)`.

`NullRows` and `OtherRows` are the same two hash tables as above. A lookup of the regular key `k` returns row numbers in the main table:

- `NullRows` returns the numbers of rows with this `k` that have a null in a mark column
- `OtherRows` returns the numbers of the other rows with this `k`. This table is built only when the left side can contain null

A row number is not enough. The mark columns of that row are compared with the left row. A null column is skipped. Every other column must be equal. At least one null has to remain. Otherwise the main hash table would already have returned `true`.

The check is:

- look up the full key `(k, a, b)` in the main hash table. A hit is `true`
- if there is no hit, look up `k` in `NullRows`. The lookup returns only the rows with this `k`. Compare the left row with them. The first row that passes the comparison makes the answer `null`
- if no compared row passes, the answer is `false`

A null in a left mark column is not looked up by the full key. That lookup cannot return `true`.

If some mark columns on the left are null and some are not, `k` is looked up in both `NullRows` and `OtherRows`. Only the rows with this `k` are compared. A null column is skipped, and every other column must be equal. Take `(1, 20, null)`. `(10, null)` from `NullRows` differs on `20` and `10`. `(20, 30)` from `OtherRows` matches on `20`, and the second column is skipped. The answer is `null`.

If every mark column on the left is null, there is nothing to compare. Any row number for this `k` makes the answer `null`. No row number makes it `false`. `(1, null, null)` finds rows for `k = 1`, so the answer is `null`.

Right side. `k = 1`: `(10, null)` and `(20, 30)`. `k = 2`: `(5, 5)`.

| left `(k, a, b)` | answer | why |
|---|---|---|
| `(1, 20, 30)` | `true` | the full key matches |
| `(1, 10, 30)` | `null` | `NullRows` returns `(10, null)`, and `10` equals `10` |
| `(1, 20, 40)` | `false` | `(10, null)` differs on the first column. `(20, 30)` is not in `NullRows` |
| `(1, 20, null)` | `null` | `OtherRows` returns `(20, 30)`, and `20` equals `20` |
| `(1, null, null)` | `null` | a row number for `k = 1` is returned |
| `(2, 5, 6)` | `false` | `NullRows` returns nothing for `k = 2` |
| `(2, 5, null)` | `null` | `OtherRows` returns `(5, 5)`, and `5` equals `5` |
| `(4, 1, 1)` | `false` | both tables return nothing for `k = 4` |

#### Complexity

- The full key hits: O(1).
- Every mark column on the left is null: O(1).
- Otherwise look up `k` in `NullRows`. If the left row has a null, look it up in `OtherRows` too. Each lookup returns only the rows with this `k`, and those rows are compared until one of them makes the answer `null`. A `false` answer has compared all of them: O(N). N is the number of rows with this `k` in the tables that were looked up.

#### Spilling
Spilling is the same as for one mark column and regular keys. The bucket is chosen by the regular key. The hash table inside the bucket is built and looked up by the full key. `NullRows` and `OtherRows` are not spilled. They are rebuilt when a spilled bucket is read back.

## Several mark columns, no regular keys

```sql
SELECT (a, b) IN (SELECT c, d FROM t2) FROM t1;
```

For one left row, a match can sit in different buckets on the right. `(10, 30)` and `(10, null)` are different full keys, so they are different buckets.

`true` and `null` should still be fast. The main hash table stays, keyed by the full key `(c, d)`. Together with three flags for the whole right side, `BuildNotEmpty_`, `BuildHasNull_`, and `BuildAllNull_`, it answers `true` and `null`.

A sure `false` has to check the other buckets too: a match there would make the answer `null` instead. The right rows are copied into their own spill buckets, filled round-robin, not by the key. The buckets spill. A row with a null mark is always copied. If the left side can contain null, every right row is copied.

If the hash table and the flags answered neither `true` nor `false`, the copied rows are compared like a cross join. Each remaining left row is checked against the copied right rows. The first right row that makes the answer `null` stops the check. If none does, the answer is `false`.

#### Complexity

- The hash table or the flags answer: O(1).
- Otherwise the cross-join comparison: O(S), where S is the number of copied right rows.

#### Spilling
The hash table spills as usual: its buckets are chosen by the full key. The copy is the change. It has its own buckets, filled round-robin, and those buckets spill too. The cross join reads them back in blocks that fit in memory.

## Not supported

- Join filters.
- Treating null as equal on a mark key.
- Building the hash table from the left side.
- More than 32 mark keys.
