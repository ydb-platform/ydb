SELECT *
FROM `/Root/S` as S
WHERE S.payload2 = String::HexDecode("54");

SELECT *
FROM `/Root/R` as R
WHERE R.ts = DateTime::MakeDate(DateTime::Parse('%Y-%m-%d')("2023-10-20"));

SELECT CAST("[6]" AS JsonDocument) as doc
FROM `/Root/R` as R;
