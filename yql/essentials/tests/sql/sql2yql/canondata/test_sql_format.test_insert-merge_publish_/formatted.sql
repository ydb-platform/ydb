/* postgres can not */
/* kikimr can not */
/* ignore plan diff */
USE plato;
PRAGMA yt.ScriptCpu = "1.0";

INSERT INTO Output1
SELECT
    "1" AS key,
    subkey,
    value
FROM Input;

INSERT INTO Output2
SELECT
    "2" AS key,
    subkey,
    value
FROM Input;

INSERT INTO Output2
SELECT
    "3" AS key,
    subkey,
    value
FROM Input;

INSERT INTO Output1
SELECT
    "4" AS key,
    subkey,
    value
FROM Input;
PRAGMA yt.ScriptCpu = "2.0";

INSERT INTO Output1
SELECT
    "5" AS key,
    subkey,
    value
FROM Input;

INSERT INTO Output1
SELECT
    "6" AS key,
    subkey,
    value
FROM Input;
