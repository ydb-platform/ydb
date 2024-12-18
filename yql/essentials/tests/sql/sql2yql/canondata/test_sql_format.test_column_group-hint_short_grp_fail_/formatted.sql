/* custom error:Expected list with at least two columns, group: "g1"*/
USE plato;

-- too short group
INSERT INTO Output WITH column_groups = '{g1=[a];def=#}'
SELECT
    *
FROM
    Input
;
