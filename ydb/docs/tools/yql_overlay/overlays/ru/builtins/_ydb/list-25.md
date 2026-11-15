### Примеры

```yql
$l = [1, 4, 7, 2];
$y = ($x, $y) -> { RETURN $x + $y; };
$z = ($x) -> { RETURN 4 * $x; };

SELECT
    ListFold($l, 6, $y) AS fold,                       -- 20
    ListFold([], 3, $y) AS fold_empty,                 -- 3
    ListFold1($l, $z, $y) AS fold1,                    -- 17
    ListFold1([], $z, $y) AS fold1_empty;              -- Null
```

### Сигнатура
