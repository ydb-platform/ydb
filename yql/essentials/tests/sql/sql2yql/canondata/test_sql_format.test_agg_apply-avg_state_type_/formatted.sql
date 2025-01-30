/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    formattype(typeof(avg(NULL))),
    formattype(typeof(avg(1))),
    formattype(typeof(avg(decimal('10', 10, 1)))),
    formattype(typeof(avg(interval('P10D')))),
    formattype(typeof(avg(just(1)))),
    formattype(typeof(avg(just(decimal('10', 10, 1))))),
    formattype(typeof(avg(just(interval('P10D')))))
FROM (
    SELECT
        1
)
GROUP BY
    ()
    WITH combine
;
