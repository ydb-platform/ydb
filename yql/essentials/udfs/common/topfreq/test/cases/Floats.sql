/* syntax version 1 */
select topfreq(x,10,10) from (
select frombytes(tobytes(-0.0),Double) as x
union all
select frombytes(tobytes(+0.0),Double) as x
union all
select Double("nan") as x
union all
select Double("nan") as x
)
