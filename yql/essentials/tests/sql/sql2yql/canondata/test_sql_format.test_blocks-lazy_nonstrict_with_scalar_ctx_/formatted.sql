USE plato;

$one =
    SELECT
        min(AssumeNonStrict(value))
    FROM Input;

$two =
    SELECT
        AssumeNonStrict(min(value))
    FROM Input;

-- fully converted to blocks - scalar context is assumed strict
SELECT
    *
FROM Input
WHERE subkey != "1" AND value > $one;

-- partially converted to blocks - AssumeStrict is calculated outside of scalar context
SELECT
    *
FROM Input
WHERE subkey != "2" AND value > $two;
