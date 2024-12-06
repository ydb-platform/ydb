/* postgres can not */
SELECT
    key,
    subkey,
    nanvl(correlation(CAST(key AS double), CAST(subkey AS double)) OVER w, NULL) AS corr,
    nanvl(covariance(CAST(key AS double), CAST(subkey AS double)) OVER w, -9.9) AS covar,
    hll(value, 18) OVER w AS hll
FROM plato.Input
WINDOW
    w AS (
        ORDER BY
            subkey
    );
