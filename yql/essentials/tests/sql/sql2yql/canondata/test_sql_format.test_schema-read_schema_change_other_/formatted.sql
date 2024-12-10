/* custom error:It is forbidden to specify the column '_other'*/
SELECT
    *
FROM
    plato.Input WITH SCHEMA Struct<_other: Yson>
;
