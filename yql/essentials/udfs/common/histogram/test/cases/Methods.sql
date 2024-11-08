/* syntax version 1 */
SELECT
    Histogram::GetSumAboveBound(histo, 5.0) AS GetSumAboveBound,
    Histogram::GetSumBelowBound(histo, 5.0) AS GetSumBelowBound,
    Histogram::CalcUpperBound(histo, 5.0) AS CalcUpperBound,
    Histogram::CalcLowerBound(histo, 5.0) AS CalcLowerBound,
    Histogram::GetSumInRange(histo, 5.0, 20.0) AS GetSumInRange
FROM (
    SELECT
        HISTOGRAM(CAST(key AS Double)) AS histo
    FROM Input
);
