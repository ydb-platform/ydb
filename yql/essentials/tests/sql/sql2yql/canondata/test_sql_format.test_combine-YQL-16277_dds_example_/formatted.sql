$min_date = ($a, $b) -> (IF($a IS NULL, $b, IF($b IS NULL, $a, IF($a < $b, $a, $b))));
$max_date = ($a, $b) -> (IF($a IS NULL OR $b IS NULL, NULL, IF($a > $b, $a, $b)));

$overlaps = ($a, $b) -> (
    IF(
        $a.date_from IS NOT NULL AND $b.date_to IS NOT NULL AND $a.date_from >= $b.date_to, FALSE,
        IF(
            $b.date_from IS NOT NULL AND $a.date_to IS NOT NULL AND $b.date_from >= $a.date_to, FALSE,
            TRUE
        )
    )
);

$f = ($key, $left, $right) -> (
    ListNotNull(
        ListFlatten(
            ListMap(
                $left, ($x) -> (
                    ListMap(
                        $right, ($y) -> (
                            IF($overlaps($x, $y), <|key: $key, value1: $x.value, value2: $y.value, date_from: $max_date($x.date_from, $y.date_from), date_to: $min_date($x.date_to, $y.date_to)|>, NULL)
                        )
                    )
                )
            )
        )
    )
);

$t1 = [
    <|date_from: '2026-01-01', date_to: '2026-01-10', id: 1, value: 'a1'|>,
    <|date_from: '2026-01-10', date_to: '2026-01-25', id: 1, value: 'a2'|>,
    <|date_from: '2026-01-25', date_to: NULL, id: 1, value: 'a3'|>,
];

$t2 = [
    <|date_from: '2026-01-01', date_to: '2026-01-20', id: 1, value: 'b1'|>,
    <|date_from: '2026-01-20', date_to: '2026-01-23', id: 1, value: 'b2'|>,
    <|date_from: '2026-01-23', date_to: NULL, id: 1, value: 'b3'|>,
];

COMBINE AS_TABLE($t1) AS t1
    PRESORT
        id,
        date_from,
        date_to
WITH AS_TABLE($t2) AS t2
    PRESORT
        id,
        date_from,
        date_to
ON
    t1.id == t2.id
USING $f(TableRow(), TableRow());
