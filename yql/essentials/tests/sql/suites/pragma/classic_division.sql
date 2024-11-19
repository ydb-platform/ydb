/* postgres can not */
SELECT 1 / 2;

PRAGMA ClassicDivision("false");
SELECT 3 / 4;
PRAGMA ClassicDivision("true");
SELECT 5 / 6;

DEFINE ACTION $div_8_by_value($value) AS
    PRAGMA ClassicDivision("false");
    SELECT 8 / $value;
END DEFINE;

DO $div_8_by_value(9);

SELECT 10 / 11;
