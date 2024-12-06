/* postgres can not */
$list =
    SELECT
        lst,
        row_number() OVER (
            ORDER BY
                lst
        ) AS rn
    FROM (
        SELECT
            *
        FROM (
            SELECT
                ListFromRange(1us, 333us) AS lst
        )
            FLATTEN LIST BY lst
    );

$usr =
    SELECT
        value,
        CAST(key AS Uint16) + 3us AS age,
        CAST(key AS Uint16) + 7us AS age1
    FROM
        plato.Input
;

SELECT
    u.*,
    l1.rn AS rn1,
    l2.rn AS rn2
FROM
    $usr AS u
JOIN
    $list AS l1
ON
    u.age == l1.lst
JOIN
    $list AS l2
ON
    u.age1 == l2.lst
ORDER BY
    value
;
