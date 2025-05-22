/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    WeakField(email, "String"),
    WeakField(yandexuid, "Uint64")
FROM Input
