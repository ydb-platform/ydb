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

`NullRows` and `OtherRows` are not spilled. When they are built and dropped is described in [Buckets and spilling](#buckets-and-spilling).

#### Complexity
Lookup: O(1). One lookup by the full key, and on a miss one lookup by the regular key. The second lookup stops at the first row with that key.

## Several mark columns and regular keys

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

## Several mark columns, no group

```sql
SELECT (a, b) IN (SELECT c, d FROM t2) FROM t1;
```

There is no second index. The main table is searched by `(c, d)` and still answers `true`.

Rows that can produce `null` are copied aside. That is every right row with a null mark, or every right row when the left side can contain null. Left rows that cannot be answered at once are set aside too. Both copies can be spilled. They are spread round-robin across the spill buckets.

Some misses are answered at once.

| left `(a, b)` | right side | answer |
|---|---|---|
| `(10, 20)` | `(10, 20)`, `(10, null)`, `(30, 40)` | `true` |
| `(null, null)` | any non-empty right side | `null` |
| any row | empty | `false` |
| any row | one right row is null in every mark column | `null` |
| `(4, 5)` | `(1, 2)`, `(3, 4)` | `false`, nobody has a null |

The other misses are compared later, in blocks that fit in memory. The comparison is the same as in the previous section.

D is the number of postponed left rows. S is the number of saved right rows. Both can be as large as the whole side.

Lookup: O(1) when the answer is known at once: a hit, an empty right side, a null in every mark column, or no nulls on either side. A postponed row is compared with every saved right row, so that lookup is O(S).

Whole join: the hash table and the immediate answers are O(L + R). The later comparison is O(D × S), so the worst case is O(L × R). Extra memory is a copy of the saved and postponed rows, and the copies spill. One comparison step holds only a block that fits in memory, but it still visits every saved row.

- `(10, 30)` against `(10, null)`: `10` equals `10`, and the second column is null. The answer is `null`.
- `(30, 50)` matches nothing. The answer is `false`.
- `(null, 20)` against `(10, 20)`: the first column is null, and `20` equals `20`. The answer is `null`.

## Buckets and spilling

The rows are split into a fixed set of buckets. A bucket is either kept in memory or written to disk.

Rows of one group stay in one bucket. Before a row is placed, the low bits of its hash are replaced with a hash of the regular key. The left row of the same group gets the same bits, so it lands in the same bucket. A group is never cut apart. One very large group cannot be spilled in pieces: the whole bucket goes to disk.

While memory is low, some buckets are written to disk. The left rows of those buckets are written too, and they are not answered yet.

The buckets that stay in memory are turned into hash tables, one bucket at a time. When a bucket's table is ready, the second index for that bucket is built from the same rows. These indexes stay until every in-memory left row has been answered. Then they are dropped. They are not written to disk.

A spilled bucket is read back later, on its own. Its hash table and its second index are built again, its left rows are answered, and the index is dropped before the next spilled bucket.

The second index has nothing of its own to spill. If memory runs out while that index is being built, the join stops.

The case with several mark columns and no group has no second index and no group. Its hash buckets are the usual ones. Rows that must be compared later are copied into their own buckets. Those buckets are filled round-robin, not by the join key, and they do spill. After the left side is read, the saved right rows are loaded back in blocks that fit in memory, and each postponed left row is compared with the current block.

## Not supported

- A comparison other than equality, such as `> ANY`.
- An extra join condition besides the keys.
- Treating null as equal on a mark key.
- Building the hash table from the left side.
- A mark key that does not fit in one column.
- More than 32 mark keys.
