/* ignore runonopt plan diff - extra PhysicalFinalizing-SuppressOuts */

USE plato;

PRAGMA config.flags('OptimizerFlags', 'ExtractOrPredicatesOverEquiJoin');

pragma yt.DisableFuseOperations;
pragma yt.DisableOptimizers="HorizontalJoin,MultiHorizontalJoin,OutHorizontalJoin";

$a = AsList(
    AsStruct(23 AS key, 3 AS subkey, Nothing(String?) AS value),
    AsStruct(37 AS key, 5 AS subkey, Nothing(String?) AS value),
    AsStruct(75 AS key, 1 AS subkey, Nothing(String?) AS value),
    AsStruct(150 AS key, 1 AS subkey, Just("AAA") AS value),
    AsStruct(150 AS key, 3 AS subkey, Just("III") AS value),
	AsStruct(150 AS key, 8 AS subkey, Just("ZZZ") AS value),
    AsStruct(200 AS key, 7 AS subkey, Nothing(String?) AS value),
    AsStruct(527 AS key, 4 AS subkey, Nothing(String?) AS value),
    AsStruct(761 AS key, 6 AS subkey, Nothing(String?) AS value),
    AsStruct(911 AS key, 2 AS subkey, Just("KKK") AS value),
);

$b = AsList(
    AsStruct(1 AS key, 1001 AS subkey, Nothing(String?) AS value),
    AsStruct(150 AS key, 150 AS subkey, Just("AAB") AS value),
    AsStruct(3 AS key, 3003 AS subkey, Nothing(String?) AS value),
    AsStruct(150 AS key, 200 AS subkey, Just("AAD") AS value),
    AsStruct(911 AS key, 5005 AS subkey, Just("AAE") AS value),
);

INSERT INTO @a SELECT * FROM AS_TABLE($a);
INSERT INTO @b SELECT * FROM AS_TABLE($b);
COMMIT;

$a_with_filter = SELECT * FROM @a WHERE key != 10;
$b_with_filter = SELECT * FROM @b WHERE key != 10;
$c_with_filter = SELECT * FROM @b WHERE key != 11;
$d_with_filter = SELECT * FROM @b WHERE key != 12;

-- Predicates for inputs

SELECT
	a.key, a.subkey, a.value, b.key, b.subkey, b.value
FROM @a AS a JOIN @b AS b ON a.key = b.key
WHERE (a.subkey = 3 AND b.subkey = 150) OR (a.subkey = 2 AND b.subkey = 5005) OR (a.subkey = 8 AND b.subkey = 200);
-- > push (a.subkey = 3 OR a.subkey = 2 OR a.subkey = 8) to a
-- > push (b.subkey = 150 OR b.subkey = 5005 OR b.subkey = 200) to b
-- > predicates for a and b must be eliminated by PhysicalOptimizer-UnessentialFilter (direct reads)

SELECT
	a.key, a.subkey, a.value, b.key, b.subkey, b.value
FROM $a_with_filter AS a JOIN @b AS b ON a.key = b.key
WHERE (a.subkey = 3 AND b.subkey = 150) OR (a.subkey = 2 AND b.subkey = 5005) OR (a.subkey = 8 AND b.subkey = 200);
-- > push (a.subkey = 3 OR a.subkey = 2 OR a.subkey = 8) to a
-- > push (b.subkey = 150 OR b.subkey = 5005 OR b.subkey = 200) to b
-- > predicates for b must be eliminated (direct read)

SELECT
	a.key, a.subkey, a.value, b.key, b.subkey, b.value
FROM $a_with_filter AS a JOIN $b_with_filter AS b ON a.key = b.key
WHERE (a.subkey = 3 AND b.subkey = 150) OR (a.subkey = 2 AND b.subkey = 5005) OR (a.subkey = 8 AND b.subkey = 200);
-- > push (a.subkey = 3 OR a.subkey = 2 OR a.subkey = 8) to a
-- > push (b.subkey = 150 OR b.subkey = 5005 OR b.subkey = 200) to b

SELECT
	a.key, a.subkey, a.value, b.key, b.subkey, b.value
FROM $a_with_filter AS a JOIN $b_with_filter AS b ON a.key = b.key
WHERE (a.subkey = 3 AND b.subkey = 150) OR a.subkey = 2 OR (a.subkey = 8 AND b.subkey = 200);
-- > push (a.subkey = 3 OR a.subkey = 2 OR a.subkey = 8) to a
-- > can't extract predicates for b

-- Constant predicates

SELECT
	a.key, a.subkey, a.value, b.key, b.subkey, b.value
FROM $a_with_filter AS a JOIN @b AS b ON a.key = b.key
WHERE (a.subkey = 3 AND b.subkey = 150 AND 'a' == 'b') OR (a.subkey = 2 AND b.subkey = 5005 AND 'c' == 'd') OR (a.subkey = 8 AND b.subkey = 200 AND 'e' == 'f');
-- > push (a.subkey = 3 OR a.subkey = 2 OR a.subkey = 8) and ('a' == 'b' OR 'c' == 'd' OR 'e' == 'f') to a
-- > push (b.subkey = 150 OR b.subkey = 5005 OR b.subkey = 200) and ('a' == 'b' OR 'c' == 'd' OR 'e' == 'f') to b
-- > predicates for b must be eliminated (direct read)

SELECT
	a.key, a.subkey, a.value, b.key, b.subkey, b.value
FROM $a_with_filter AS a JOIN $b_with_filter AS b ON a.key = b.key
WHERE (a.subkey = 3 AND b.subkey = 150 AND 'a' == 'b') OR (a.subkey = 2 AND b.subkey = 5005 AND 'c' == 'd') OR (a.subkey = 8 AND b.subkey = 200 AND 'e' == 'f');
-- > push (a.subkey = 3 OR a.subkey = 2 OR a.subkey = 8) and ('a' == 'b' OR 'c' == 'd' OR 'e' == 'f') to a
-- > push (b.subkey = 150 OR b.subkey = 5005 OR b.subkey = 200) and ('a' == 'b' OR 'c' == 'd' OR 'e' == 'f') to b

SELECT
	a.key, a.subkey, a.value, b.key, b.subkey, b.value
FROM $a_with_filter AS a JOIN $b_with_filter AS b ON a.key = b.key
WHERE (a.subkey = 3 AND b.subkey = 150 AND 'a' == 'b') OR (a.subkey = 2 AND b.subkey = 5005) OR (a.subkey = 8 AND b.subkey = 200 AND 'e' == 'f');
-- > push (a.subkey = 3 OR a.subkey = 2 OR a.subkey = 8) to a
-- > push (b.subkey = 150 OR b.subkey = 5005 OR b.subkey = 200) to b
-- > can't extract constant predicates

-- Predicate expansion

SELECT
	a.key, b.key, c.key, d.key
FROM $a_with_filter AS a
JOIN $b_with_filter AS b ON a.key = b.key
JOIN $c_with_filter AS c ON b.key = c.key
JOIN $d_with_filter AS d ON c.key = d.key
WHERE
    (a.subkey = 1 AND b.subkey = 3 AND c.subkey = 6 AND d.subkey = 10)
    OR (
        a.subkey = 2
        AND (
            (b.subkey = 4 AND c.subkey = 7 AND d.subkey = 11)
            OR (
                b.subkey = 5
                AND ((c.subkey = 8 AND d.subkey = 12) OR (c.subkey = 9 AND d.subkey = 13))
            )
        )
    );
-- > push (a.subkey IN (1, 2, 2, 2)) to a
-- > push (b.subkey IN (3, 4, 5, 5)) to b
-- > push (c.subkey IN (6, 7, 8, 9)) to c
-- > push (d.subkey IN (10, 11, 12, 13)) to d

-- Strict

SELECT
	a.key, a.subkey, a.value, b.key, b.subkey, b.value
FROM $a_with_filter AS a JOIN $b_with_filter AS b ON a.key = b.key
WHERE (a.key = 1 AND Unwrap(b.value) = "AAA" and b.key = 1) OR (a.value = Just("AAA") AND b.key = 2);
-- > push (a.key = 1 OR a.value = Just("AAA")) to a
-- > can't extract predicates for b

-- Misc

SELECT
	a.key, a.subkey, a.value, b.key, b.subkey, b.value
FROM $a_with_filter AS a JOIN $b_with_filter AS b ON a.key = b.key
WHERE (a.subkey = b.key AND 'a' == 'b') OR (a.subkey = 2 AND 'c' == 'd') OR (a.subkey = 8 AND 'e' == 'f');
-- > push ('a' == 'b' OR 'c' == 'd' OR 'e' == 'f') to a
-- > push ('a' == 'b' OR 'c' == 'd' OR 'e' == 'f') to b
-- > can't extract non-constant predicates for inputs

SELECT
	a.key, a.subkey, a.value, b.key, b.subkey, b.value
FROM $a_with_filter AS a JOIN $b_with_filter AS b ON a.key = b.key
WHERE (a.subkey = 3 AND b.subkey = 150) OR (a.subkey = 2 AND b.subkey = 5005 AND RandomNumber(JoinTableRow()) == 0) OR (a.subkey = 8 AND b.subkey = 200);
-- > can't extract predicates for inputs (JoinTableRow() prevents extraction)
