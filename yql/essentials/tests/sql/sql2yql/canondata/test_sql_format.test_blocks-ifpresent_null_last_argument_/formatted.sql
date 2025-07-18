$data = [
    <|optionalValue: Just(4)|>,
    <|optionalValue: Nothing(int32?)|>,
    <|optionalValue: Just(404)|>,
    <|optionalValue: Nothing(int32?)|>,
];

$f = ($x) -> {
    RETURN Just($x * 2);
};

SELECT
    YQL::IfPresent(optionalValue, $f, NULL)
FROM
    as_table($data)
;
