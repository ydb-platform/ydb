/* syntax version 1 */
USE plato;

$src = [
 <|x:1,  idx:1|>,
 <|x:7,  idx:2|>,
 <|x:5,  idx:3|>,
 <|x:4,  idx:4|>,
 <|x:3,  idx:5|>,
 <|x:11, idx:6|>,
 <|x:2,  idx:7|>,
 <|x:11, idx:8|>,
 <|x:0,  idx:9|>,
 <|x:6,  idx:10|>,
];

INSERT INTO @src
SELECT * FROM AS_TABLE($src) ORDER BY idx;

COMMIT;

SELECT idx, x, TOP(x, 3)    OVER (ORDER BY idx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as upcr_top FROM @src ORDER BY idx;
SELECT idx, x, TOP(x, 3)    OVER ()                                                              as upuf_top FROM @src ORDER BY idx;
SELECT idx, x, TOP(x, 3)    OVER (ORDER BY idx ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as cruf_top FROM @src ORDER BY idx;
SELECT idx, x, TOP(x, 3)    OVER (ORDER BY idx ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)         as cr22_top FROM @src ORDER BY idx;

SELECT idx, x, BOTTOM(x, 3) OVER (ORDER BY idx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as upcr_bottom FROM @src ORDER BY idx;
SELECT idx, x, BOTTOM(x, 3) OVER ()                                                              as upuf_bottom FROM @src ORDER BY idx;
SELECT idx, x, BOTTOM(x, 3) OVER (ORDER BY idx ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as cruf_bottom FROM @src ORDER BY idx;
SELECT idx, x, BOTTOM(x, 3) OVER (ORDER BY idx ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)         as cr22_botto FROM @src ORDER BY idx;
