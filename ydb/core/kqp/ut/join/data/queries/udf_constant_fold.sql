SELECT *
FROM `/Root/S` as S
WHERE S.payload2 = String::HexDecode("54");

SELECT CAST("[6]" AS JsonDocument) as doc
FROM `/Root/R` as R;

SELECT CAST("1.23" AS DyNumber) as doc
FROM `/Root/R` as R;

SELECT *
FROM `/Root/R` as R
WHERE CAST("1.32" AS DyNumber) = CAST("1.23" AS DyNumber);

