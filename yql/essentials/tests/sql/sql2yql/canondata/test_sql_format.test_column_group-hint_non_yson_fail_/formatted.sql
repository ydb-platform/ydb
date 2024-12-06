/* custom error:Unexpected '!' while parsing node*/
USE plato;

-- bad yson
INSERT INTO Output WITH column_groups = "!"
SELECT
    *
FROM Input;
