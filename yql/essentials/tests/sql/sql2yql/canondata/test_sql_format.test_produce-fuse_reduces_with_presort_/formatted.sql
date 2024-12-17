USE plato;

$reduce = ($_, $TableRows) -> {
    RETURN Yql::Condense1(
        $TableRows,
        ($item) -> ($item),
        ($_, $_) -> (FALSE),
        ($item, $_) -> ($item)
    );
};

$stream = (
    SELECT
        *
    FROM
        Input
);

--
$stream1 = (
    REDUCE $stream
    PRESORT
        value1
    ON
        key,
        subkey
    USING $reduce(TableRow())
    ASSUME ORDER BY
        key,
        subkey,
        value1
);

$stream1 = (
    REDUCE $stream1
    PRESORT
        value1
    ON
        key,
        subkey
    USING $reduce(TableRow())
    ASSUME ORDER BY
        key,
        subkey,
        value1
);

--
$stream2 = (
    REDUCE $stream
    PRESORT
        value1,
        value2
    ON
        key,
        subkey
    USING $reduce(TableRow())
    ASSUME ORDER BY
        key,
        subkey,
        value1,
        value2
);

$stream2 = (
    REDUCE $stream2
    PRESORT
        value1
    ON
        key,
        subkey
    USING $reduce(TableRow())
    ASSUME ORDER BY
        key,
        subkey,
        value1
);

--
$stream3 = (
    REDUCE $stream
    PRESORT
        value1,
        value2,
        value3
    ON
        key,
        subkey
    USING $reduce(TableRow())
    ASSUME ORDER BY
        key,
        subkey,
        value1,
        value2,
        value3
);

$stream3 = (
    REDUCE $stream3
    ON
        key,
        subkey
    USING $reduce(TableRow())
    ASSUME ORDER BY
        key,
        subkey
);

SELECT
    *
FROM
    $stream1
ASSUME ORDER BY
    `key`,
    `subkey`
;

SELECT
    *
FROM
    $stream2
ASSUME ORDER BY
    `key`,
    `subkey`
;

SELECT
    *
FROM
    $stream3
ASSUME ORDER BY
    `key`,
    `subkey`
;
