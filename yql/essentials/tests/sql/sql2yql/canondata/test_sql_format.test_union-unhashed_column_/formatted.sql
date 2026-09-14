/* custom error: Expected hashable and equatable type for column: x, but got: Yson */
SELECT
    "1"y AS x
UNION
SELECT
    "2"y AS x
;
