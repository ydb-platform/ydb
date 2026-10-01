$input = [
    <|value: "ываыва"u|>,
    <|value: "ячсячсяаачы"u|>,
    <|value: "аавыаываыва"u|>,
    <|value: "gd2цй3ываафы"u|>,
    <|value: ""u|>,
];

SELECT
    value as value,
    Unicode::RemoveAll(value, "фа"u) AS all,
    Unicode::RemoveFirst(value, "а"u) AS first,
    Unicode::RemoveLast(value, "а"u) AS last,
    Unicode::RemoveFirst(value, "фа"u) AS first2,
    Unicode::RemoveLast(value, "фа"u) AS last2
FROM AS_TABLE($input);
