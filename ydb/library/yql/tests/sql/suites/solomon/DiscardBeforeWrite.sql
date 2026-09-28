-- DQ-52: source settings must not pull the previous DISCARD's stages
-- into the execution that writes the second Solomon read.
USE plato;

DEFINE SUBQUERY $read_max() AS
    SELECT MAX(ts) AS max_ts
    FROM local_solomon.my_project WITH (
        program = @@{}@@,
        from = '1970-01-01T00:00:01Z',
        to = '1970-01-01T00:00:31Z'
    );
END DEFINE;

$max = (SELECT max_ts FROM $read_max());

DISCARD SELECT Ensure(
    TRUE,
    $max < Datetime('1970-01-01T00:00:31Z'),
    'Expected metrics before the interval end'
) FROM (SELECT 1);

INSERT INTO @out
SELECT ts, value
FROM local_solomon.my_project WITH (
    program = @@{}@@,
    from = '1970-01-01T00:00:01Z',
    to = '1970-01-01T00:00:31Z'
);

COMMIT;
SELECT * FROM @out;
