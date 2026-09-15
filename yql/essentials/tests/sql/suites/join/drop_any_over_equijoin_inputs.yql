PRAGMA config.flags('OptimizerFlags', 'DropAnyOverEquiJoinInputs');

$normal_data = AsList(
    AsStruct(1 AS key, 1001 AS subkey, "AAA" AS value),
    AsStruct(1 AS key, 1001 AS subkey, "AAB" AS value),
    AsStruct(1 AS key, 1002 AS subkey, "AAC" AS value),
    AsStruct(2 AS key, 1002 AS subkey, "AAD" AS value),
    AsStruct(NULL AS key, NULL AS subkey, "AAE" AS value),
    AsStruct(NULL AS key, NULL AS subkey, "AAF" AS value),
);
$normal = (SELECT * FROM AS_TABLE($normal_data));

$distinct_data = AsList(
    AsStruct(1 AS key, 1001 AS subkey, "AAA" AS value),
    AsStruct(1 AS key, 1002 AS subkey, "AAC" AS value),
    AsStruct(2 AS key, 1002 AS subkey, "AAD" AS value),
    AsStruct(NULL AS key, NULL AS subkey, "AAE" AS value),
);
$distinct = (SELECT /*+ distinct(key subkey) */ * FROM AS_TABLE($distinct_data));

$unique_data = AsList(
    AsStruct(1 AS key, 1001 AS subkey, "AAA" AS value),
    AsStruct(1 AS key, 1002 AS subkey, "AAC" AS value),
    AsStruct(2 AS key, 1002 AS subkey, "AAD" AS value),
    AsStruct(NULL AS key, NULL AS subkey, "AAE" AS value),
    AsStruct(NULL AS key, NULL AS subkey, "AAF" AS value),
);
$unique = (SELECT /*+ unique(key subkey) */ * FROM AS_TABLE($unique_data));

$complex_key_data = AsList(
    AsStruct(AsTuple(1, 1001) AS key, "AAA" AS value),
    AsStruct(AsTuple(1, 1002) AS key, "AAC" AS value),
    AsStruct(AsTuple(2, 1002) AS key, "AAD" AS value),
    AsStruct(AsTuple(NULL, NULL) AS key, "AAE" AS value),
);
$complex_key = (SELECT /*+ distinct(key) */ * FROM AS_TABLE($complex_key_data));

-------------------------------------------------------------------------------

-- keep - key columns set doesn't contain distinct set
SELECT
    a.key, a.subkey, a.value,
    b.key, b.subkey, b.value
FROM $normal AS a
JOIN ANY $distinct AS b ON a.key = b.key;

-- drop - distinct by key columns
SELECT
    a.key, a.subkey, a.value,
    b.key, b.subkey, b.value
FROM $normal AS a
JOIN ANY $distinct AS b ON a.key = b.key AND a.subkey = b.subkey;

-- drop - distinct by key columns
SELECT
    a.key, a.subkey, a.value,
    b.key, b.subkey, b.value
FROM $normal AS a
JOIN ANY $distinct AS b ON a.key = b.key AND a.subkey = b.subkey AND a.value = b.value;

-------------------------------------------------------------------------------

-- drop - any over unique inner join side
SELECT
    a.key, a.subkey, a.value,
    b.key, b.subkey, b.value
FROM $normal AS a
JOIN ANY $unique AS b ON a.key = b.key AND a.subkey = b.subkey;

-- drop - any over unique right side of left join
SELECT
    a.key, a.subkey, a.value,
    b.key, b.subkey, b.value
FROM $normal AS a
LEFT JOIN ANY $unique AS b ON a.key = b.key AND a.subkey = b.subkey;

-- drop - any over unique left side of right join
SELECT
    a.key, a.subkey, a.value,
    b.key, b.subkey, b.value
FROM ANY $unique AS a
RIGHT JOIN $normal AS b ON a.key = b.key AND a.subkey = b.subkey;

-- keep - any over unique left side of left join
SELECT
    a.key, a.subkey, a.value,
    b.key, b.subkey, b.value
FROM ANY $unique AS a
LEFT JOIN $normal AS b ON a.key = b.key AND a.subkey = b.subkey;

-- keep - any over unique right side of right join
SELECT
    a.key, a.subkey, a.value,
    b.key, b.subkey, b.value
FROM $normal AS a
RIGHT JOIN ANY $unique AS b ON a.key = b.key AND a.subkey = b.subkey;

-------------------------------------------------------------------------------

SELECT
    a.key, a.subkey, a.value,
    b.key, b.subkey, b.value,
    c.key, c.subkey, c.value,
    d.key, d.subkey, d.value,
    e.key, e.subkey, e.value
FROM $normal AS a
JOIN ANY $distinct AS b ON a.key = b.key -- keep
JOIN ANY $distinct AS c ON a.key = c.key AND a.subkey = c.subkey -- drop
RIGHT JOIN ANY $unique AS d ON a.key = d.key AND a.subkey = d.subkey -- keep
LEFT JOIN ANY $unique AS e ON a.key = e.key AND a.subkey = e.subkey; -- drop

-------------------------------------------------------------------------------

SELECT
    a.key, a.subkey, a.value,
    b.key.0, b.key.1, b.value
FROM $normal AS a
JOIN ANY $complex_key AS b ON a.key = b.key.0 AND a.subkey = b.key.1; -- drop
