/* custom error:Column group "g1" refers to unknown column "l"*/
USE plato;

-- unknown column
INSERT INTO Output WITH column_groups = "{g1=[l;b;c];def=#}"
SELECT
    *
FROM
    Input
;
