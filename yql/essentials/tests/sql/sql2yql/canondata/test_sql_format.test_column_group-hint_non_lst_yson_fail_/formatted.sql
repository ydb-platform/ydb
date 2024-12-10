/* custom error:Expected list value, group: "g1"*/
USE plato;

-- bad yson
INSERT INTO Output WITH column_groups = @@{g1="a"}@@
SELECT
    *
FROM
    Input
;
