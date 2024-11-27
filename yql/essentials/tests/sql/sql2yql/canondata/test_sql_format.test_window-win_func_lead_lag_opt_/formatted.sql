/* postgres can not */
USE plato;

SELECT
    subkey,
    lag(Just(subkey)) OVER w AS opt_lag,
    lead(Just(subkey)) OVER w AS opt_lead,
    lag(subkey, 0) OVER w AS lag0,
    lead(subkey, 0) OVER w AS lead0
FROM Input
WINDOW
    w AS ()
ORDER BY
    subkey;

SELECT
    key,
    lag(optkey) OVER w AS opt_lag,
    lead(Just(optkey)) OVER w AS opt_lead,
    lag(Just(optkey), 0) OVER w AS lag0,
    lead(optkey, 0) OVER w AS lead0
FROM InputOpt
WINDOW
    w AS ()
ORDER BY
    key;

SELECT
    lead(NULL) OVER w
FROM (
    SELECT
        1 AS key
)
WINDOW
    w AS ();
