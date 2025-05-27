PRAGMA warning('disable', '4510');

$init = ListCreate(Struct<n: Int32>);

$transform = ($value) -> {
    RETURN ListMap(ListFilter($value, ($r) -> ($r.n < 5)), ($r) -> (<|n: $r.n + 1|>));
};

SELECT
    *
FROM
    AS_TABLE(Yql::PgIterateAll($init, $transform))
ORDER BY
    n
;

$init = [<|n: 1|>];

$transform = ($value) -> {
    RETURN ListMap(ListFilter($value, ($r) -> ($r.n < 5)), ($r) -> (<|n: $r.n + 1|>));
};

SELECT
    *
FROM
    AS_TABLE(Yql::PgIterateAll($init, $transform))
ORDER BY
    n
;

$init = [<|n: 1|>, <|n: 1|>, <|n: 2|>];

$transform = ($value) -> {
    RETURN ListFlatMap($value, ($_r) -> ([<|n: 1|>, <|n: 2|>, <|n: 2|>]));
};

SELECT
    *
FROM
    AS_TABLE(Yql::PgIterate($init, $transform))
ORDER BY
    n
;
