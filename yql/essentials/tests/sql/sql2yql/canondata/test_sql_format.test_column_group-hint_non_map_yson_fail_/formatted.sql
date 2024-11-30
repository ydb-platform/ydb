/* custom error:Expected Yson map, got: list_node*/
USE plato;

-- bad yson
INSERT INTO Output WITH column_groups = "[abc]"
SELECT
    *
FROM Input;
