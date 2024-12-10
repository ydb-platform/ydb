/* postgres can not */
$optDuoFloat = Just(Just(CAST(2.71 AS float)));
$optDuoDouble = Just(Just(CAST(3.14 AS double)));
$optDuoFloatN = Just(Just(CAST(NULL AS float)));
$optDuoDoubleN = Just(Just(CAST(NULL AS double)));

SELECT
    ($optDuoFloat ?? 0) ?? 1,
    ($optDuoDouble ?? 41) ?? 42,
    ($optDuoFloatN ?? 0) ?? 1.,
    ($optDuoDoubleN ?? 41.) ?? 42,
    ($optDuoFloatN ?? CAST(40.1 AS float)) ?? CAST(40.2 AS float),
    ($optDuoDoubleN ?? CAST(40.1 AS float)) ?? CAST(40.2 AS float),
    (($optDuoFloatN ?? 0) ?? 1.) ?? 3,
    (($optDuoDoubleN ?? 41) ?? 42) ?? 4,
    'end'
FROM
    plato.Input
;
