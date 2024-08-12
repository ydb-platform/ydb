SELECT *
FROM `/Root/R` as R
WHERE CAST(R.ts AS Timestamp) = (CAST('1998-12-01' AS Date) - Interval("P100D"))
