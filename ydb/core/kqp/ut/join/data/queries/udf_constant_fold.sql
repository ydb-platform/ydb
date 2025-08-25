SELECT *
FROM `/Root/S` as S
WHERE S.payload2 = String::HexDecode("54");

SELECT *
FROM `/Root/R` as R
WHERE R.ts = DateTime::MakeDate(DateTime::Parse('%Y-%m-%d')("2023-10-20"));

SELECT CAST("[6]" AS JsonDocument) as doc
FROM `/Root/R` as R;

SELECT CAST("1.23" AS DyNumber) as doc
FROM `/Root/R` as R;

SELECT *
FROM `/Root/R` as R
WHERE CAST("1.32" AS DyNumber) = CAST("1.23" AS DyNumber);

