SELECT *
FROM `/Root/S` as S
WHERE S.payload2 = String::HexDecode("54");

SELECT CAST("[6]" AS JsonDocument) as doc
FROM `/Root/R` as R;
