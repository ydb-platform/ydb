PRAGMA DqEngine = "auto";

SELECT
    *
FROM
    AS_TABLE(ListMap(ListFromRange(1, 10000), ($x) -> (<|a: $x|>)))
;
