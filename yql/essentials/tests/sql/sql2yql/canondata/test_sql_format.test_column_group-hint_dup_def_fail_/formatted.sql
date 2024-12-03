/* custom error:Not more than one group should have # value: "def2"*/
USE plato;

-- duplicate column
INSERT INTO Output WITH column_groups = "{def1=#;def2=#}"
SELECT
    *
FROM Input;
