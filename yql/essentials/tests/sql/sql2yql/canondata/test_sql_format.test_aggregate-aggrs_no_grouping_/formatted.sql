/* syntax version 1 */
/* postgres can not */
SELECT
    count(key) AS keyCount,
    count(sub) AS subCount,
    count(val) AS valCount,
    countIf(sub % 2 == 0) AS evenCount,
    countIf(sub % 2 == 1) AS oddCount,
    every(sub % 2 == 0) AS every,
    boolOr(sub % 2 == 0) AS boolOr,
    avg(key) AS keyAvg,
    avg(sub) AS subAvg,
    min(key) AS keyMin,
    min(sub) AS subMin,
    min(val) AS valMin,
    max(key) AS keyMax,
    max(sub) AS subMax,
    max(val) AS valMax,
    some(key) AS keySome,
    some(sub) AS subSome,
    some(val) AS valSome,
    bitAnd(CAST(key AS Uint64)) AS keyBitAnd,
    bitOr(CAST(key AS Uint64)) AS keyBitOr,
    bitXor(CAST(key AS Uint64)) AS keyBitXor,
    bitAnd(CAST(sub AS Uint64)) AS subBitAnd,
    bitOr(CAST(sub AS Uint64)) AS subBitOr,
    bitXor(CAST(sub AS Uint64)) AS subBitXor,
    median(key) AS keyMedian,
    median(sub) AS subMedian,
    stdDev(key) AS keyStdDev,
    stdDev(sub) AS subStdDev,
    stdDev(empty) AS emptyStdDev,
    variance(key) AS keyVariance,
    variance(sub) AS subVariance,
    stdDevPop(key) AS keyPopStdDev,
    stdDevPop(sub) AS subPopStdDev,
    varPop(key) AS keyPopVariance,
    varPop(sub) AS subPopVariance,
    correlation(key, sub) AS corr,
    covariance(key, sub) AS covar,
    covarpop(key, sub) AS covarpop
FROM (
    SELECT
        CAST(key AS int) AS key,
        Unwrap(CAST(subkey AS int)) AS sub,
        value AS val,
        CAST(value AS int) AS empty
    FROM
        plato.Input
);
