$T = '';
$O = Nothing(String?);
$U = InstanceOf(YQL::UniversalType());

$coalesce = ($lhs, $rhs, $res) -> {
    $expectedType = FormatType(TypeOf($res));
    $res = FormatType(TypeOf($lhs ?? $rhs));
    $res = Ensure($res, $res == $expectedType, $res || ' != ' || $expectedType);
    RETURN <|
        lhs: FormatType(TypeOf($lhs)),
        rhs: FormatType(TypeOf($rhs)),
        res: $res,
    |>;
};

SELECT
    lhs,
    rhs,
    res
FROM
    AsTable([
        $coalesce($T, $T, $T),
        $coalesce($T, $O, $T),
        $coalesce($T, $U, $T),
        $coalesce($O, $T, $T),
        $coalesce($O, $O, $O),
        $coalesce($O, $U, $U),
        $coalesce($U, $T, $U),
        $coalesce($U, $O, $U),
        $coalesce($U, $U, $U),
    ])
;
