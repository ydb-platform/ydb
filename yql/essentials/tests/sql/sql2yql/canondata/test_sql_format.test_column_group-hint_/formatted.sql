USE plato;

$i1 = (
    SELECT
        *
    FROM
        Input
    WHERE
        a > 'a'
); -- several publish consumers with same groups

$i2 = (
    SELECT
        *
    FROM
        Input
    WHERE
        a > 'a1'
); -- several publish consumers with different groups

$i3 = (
    SELECT
        *
    FROM
        Input
    WHERE
        a < 'a2'
); -- several consumers including publish

$i4 = (
    SELECT
        *
    FROM
        Input
    WHERE
        a != 'a'
); -- several publish consumers with and without groups

-- test column group spec normalization
INSERT INTO Output1 WITH column_groups = '{g1=[a;b;c];def=#}'
SELECT
    *
FROM
    $i1
;

INSERT INTO Output1 WITH column_groups = '{def=#;g1=[c;a;b];}'
SELECT
    *
FROM
    $i2
;

INSERT INTO Output2 WITH column_groups = '{def=#}'
SELECT
    *
FROM
    $i2
;

INSERT INTO Output2 WITH column_groups = '{def=#}'
SELECT
    *
FROM
    $i3
;

INSERT INTO Output3 WITH column_groups = '{g1=[a;b;c];def=#}'
SELECT
    *
FROM
    $i1
;

INSERT INTO Output3 WITH column_groups = '{g1=[a;b;c];def=#}'
SELECT
    *
FROM
    $i4
;

INSERT INTO Output4
SELECT
    *
FROM
    $i4
;

SELECT
    a,
    b,
    c,
    d
FROM
    $i3
;
