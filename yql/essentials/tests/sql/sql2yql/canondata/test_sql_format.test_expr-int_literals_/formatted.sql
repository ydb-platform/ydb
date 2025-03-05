/* postgres can not */
SELECT
    23 AS small_int,
    9262583611491805930 AS unsigned_long,
    0x7fffffff AS i32max,
    0x80000000 AS i32max_plus1,
    0x7fffffffffffffff AS i64max,
    0x8000000000000000 AS i64max_plus1
;
