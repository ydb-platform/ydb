/* syntax version 1 */
-- Expect: guest ythrow yexception() → host sees terminate / UDF error.
-- Contrast with Throw::fail() which uses the ThrowException host import.
SELECT
    Yexception::fail() AS x;
