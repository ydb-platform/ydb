/* custom error:Expected string value in list, found int64_node, group: "g1"*/
USE plato;

-- bad yson
INSERT INTO Output WITH column_groups = "{g1=[3;a]}"
SELECT
    *
FROM
    Input
;
