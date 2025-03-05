/* postgres can not */
SELECT
  AsTuple(1,1) -- tuple is unsupported
IN
  (AsTuple(1, 1))
