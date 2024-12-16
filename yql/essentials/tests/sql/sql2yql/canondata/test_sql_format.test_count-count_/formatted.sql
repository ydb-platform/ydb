/* syntax version 1 */
SELECT
    a,
    aggregate_list(b),
    count(*)
FROM
    plato.Input
GROUP BY
    a
ORDER BY
    a
;

SELECT
    b,
    aggregate_list(a),
    count(*)
FROM
    plato.Input
GROUP BY
    b
ORDER BY
    b
;

SELECT
    c,
    aggregate_list(a),
    count(*)
FROM
    plato.Input
GROUP BY
    c
ORDER BY
    c
;

SELECT
    d,
    aggregate_list(a),
    count(*)
FROM
    plato.Input
GROUP BY
    d
ORDER BY
    d
;

SELECT
    e,
    aggregate_list(a),
    count(*)
FROM
    plato.Input
GROUP BY
    e
ORDER BY
    e
;
