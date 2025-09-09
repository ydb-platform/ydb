/* postgres can not */
USE plato;

$sub_raws = (SELECT ROW_NUMBER() OVER trivialWindow as RowNum
FROM Input WINDOW trivialWindow As ()
);

--INSERT INTO Output
SELECT
  *
FROM $sub_raws;

SELECT
  *
FROM $sub_raws;
