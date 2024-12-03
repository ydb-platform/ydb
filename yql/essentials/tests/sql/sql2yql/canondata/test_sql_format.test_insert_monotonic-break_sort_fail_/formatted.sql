/* custom error:Inserts with "monotonic_keys" setting must not change output table sorting*/
USE plato;

INSERT INTO Output WITH MONOTONIC_KEYS
SELECT
    *
FROM Input1;
