/* custom error:Insert with "column_groups" to existing table is not allowed*/
USE plato;

INSERT INTO Output WITH column_groups = "{g1=[a;b];def=#}"
SELECT
    *
FROM Input;
