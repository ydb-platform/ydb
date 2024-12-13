/* custom error:All appends within the same commit should have the equal "column_groups" value*/
USE plato;

INSERT INTO Output WITH column_groups = "{g1=[a;b];def=#}"
SELECT
    *
FROM
    Input
;

INSERT INTO Output WITH column_groups = "{g1=[c;d];def=#}"
SELECT
    *
FROM
    Input
;
