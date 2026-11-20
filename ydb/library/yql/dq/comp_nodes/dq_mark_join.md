# Mark join

A mark join answers `IN` or `EXISTS` for every left row. The left row stays. One extra column says `true`, `false`, or `null`.

The right side is placed in a hash table. A hit in that table is `true`. The rest of this note is about a miss: the answer is then either `null` or `false`.

There are two kinds of keys.

- A regular key puts rows in a group. In `t2.k = t1.k`, `k` is the regular key. If nulls are allowed to match, two nulls are one group. Otherwise a null regular key means `false`.
- A mark key is a column of the `IN` test. A null there stays unknown. A mark key cannot treat null as equal to null.

There are at most 32 mark keys, and each one is a single column. The optimizer does not produce this join yet. The tests in `ut/dq_hash_join_ut.cpp` call it directly.

## One mark column, no group

```sql
SELECT x IN (SELECT y FROM t2) FROM t1;
```

While the hash table is built, the join remembers two facts: the right side is not empty, and some `y` is null.

| `x` | right side | answer |
|---|---|---|
| `1` | `1`, null, `2` | `true` |
| `3` | `1`, null, `2` | `null` |
| null | `1`, null, `2` | `null` |
| `3` | `1`, `2` | `false` |
| null | empty | `false` |

One null on the right answers every miss the same way, so the right side is not read again.

Cost: each side is read once, O(L + R). L is the number of left rows, R is the number of right rows. A miss checks two booleans, which is O(1). Extra memory is those two booleans.

## One mark column, with a group

```sql
SELECT x IN (SELECT y FROM t2 WHERE t2.k = t1.k) FROM t1;
```

A null in one group does not affect another group. Rows with the same `k` go into the same bucket.

The rows are stored once, in the main hash table, under the full key `(k, y)`. A second index points at those same rows and is searched by `k` only. The slot holds the hash of `k` and the row number. The row is not copied. The index has two parts:

- rows whose `y` is null;
- the other rows, and only when the left side can contain null.

Finding any row of the group is enough. `y` is not compared again.

Right side: `(1, 10)`, `(1, null)`, `(2, 20)`.

| left `(k, x)` | answer |
|---|---|
| `(1, 10)` | `true`, the full key matches |
| `(1, 30)` | `null`, group 1 has a null |
| `(1, null)` | `null`, group 1 exists |
| `(2, 10)` | `false`, group 2 has no null |
| `(3, null)` | `false`, group 3 does not exist |

A missing group is `false` even when `x` is null. The subquery for that `k` returned nothing.

If the mark column cannot be null on either side, the second index is not built. A miss is `false`. If only the right side can contain null, the index keeps groups that have a null `y`.

The second index is not spilled. It lives only while its bucket is in memory, and it is freed before the next bucket. One group always sits in one bucket, so spilling does not cut it apart.

Cost: the main table and the second index are built in one pass over the right side. Each left row does one lookup by the full key and one lookup by `k`. The lookup by `k` stops at the first row of the group, so a large group does not make it slower. The total is O(L + R). Extra memory is one hash and one row number per indexed right row of the buckets that are in memory. The row itself is not copied.

## Several mark columns, with a group

```sql
SELECT (a, b) IN (SELECT c, d FROM t2 WHERE t2.k = t1.k) FROM t1;
```

The main table is searched by `(k, c, d)`. The second index is the same as above and is still searched by `k`. After a miss, the rows of that group are compared.

A null column is skipped. Every other column must be equal. Some null has to remain. Otherwise the main table would already have returned `true`.

If every mark column on the left is null, there is nothing to compare. The group exists, so the answer is `null`.

If the left row has no null, only the right rows with a null mark are compared. The other rows of the group are compared only when the left row itself has a null.

Group `k = 1` contains `(10, null)` and `(20, 30)`. Group `k = 2` contains `(5, 5)`.

| left `(k, a, b)` | answer | why |
|---|---|---|
| `(1, 20, 30)` | `true` | the full key matches |
| `(1, 10, 30)` | `null` | `(10, null)` agrees on `10` |
| `(1, 20, 40)` | `false` | `(10, null)` differs on the first column |
| `(1, 20, null)` | `null` | `(20, 30)` agrees on `20` |
| `(1, null, null)` | `null` | the group exists |
| `(2, 5, 6)` | `false` | group 2 has no null mark |
| `(2, 5, null)` | `null` | `(5, 5)` agrees on `5` |
| `(4, 1, 1)` | `false` | there is no such group |

Cost: building both indexes is the same as above, O(L + R). If every mark column on the left is null, one group lookup is enough. Otherwise a miss reads rows of its group until it finds a null answer or the rows run out. A false answer has read every relevant row of the group. The worst case is O(L × G), where G is the number of rows in that group. Extra memory is the same as above: a hash and a row number per indexed row, not a copy of the row. DuckDB does not run this query.

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

Cost: the exact lookup and the answers that are known at once are O(L + R). The later comparison reads every postponed left row against every saved right row, O(D × S). D is the number of postponed left rows, S is the number of saved right rows. Both can be as large as the whole side, so the worst case is O(L × R). Extra memory is a copy of those rows, and the copies spill. One comparison step holds only a block that fits in memory, but it still visits every saved row.

- `(10, 30)` against `(10, null)`: `10` equals `10`, and the second column is null. The answer is `null`.
- `(30, 50)` matches nothing. The answer is `false`.
- `(null, 20)` against `(10, 20)`: the first column is null, and `20` equals `20`. The answer is `null`.

## Spilling

The hash join spills its buckets and its left pages as usual. With no group, the saved right rows and the postponed left rows spill too. The second index does not spill. It is built for the bucket that is in memory and freed before the next one.

## Not supported

- A comparison other than equality, such as `> ANY`.
- An extra join condition besides the keys.
- Treating null as equal on a mark key.
- Building the hash table from the left side.
- A mark key that does not fit in one column.
- More than 32 mark keys.
