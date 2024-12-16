/* custom error:Expected list with at least two columns, group: "g1"*/
USE plato;

-- empty group
INSERT INTO Output WITH column_groups = "{g1=[];def=#}"
SELECT
    *
FROM
    Input
;
