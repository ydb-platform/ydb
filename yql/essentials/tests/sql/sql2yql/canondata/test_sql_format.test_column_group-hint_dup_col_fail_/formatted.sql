/* custom error:Duplicate column "a"*/
USE plato;

-- duplicate column
INSERT INTO Output WITH column_groups = "{g1=[a;a;b];def=#}"
SELECT
    *
FROM Input;
