/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
  countIf(kkstritem % 10 == 0) as count_dec,
  countIf(kkstritem < 100) as count_small,
  countIf(kkstritem > 900) as count_huge
FROM (
      SELECT keyitem.kk as kkstr FROM Input as d FLATTEN BY key as keyitem
)
FLATTEN BY kkstr as kkstritem
