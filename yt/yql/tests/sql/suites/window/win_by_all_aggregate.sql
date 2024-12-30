/* postgres can not */
/* syntax version 1 */
use plato;

$data = (select cast(key as uint32) as age, cast(subkey as uint32) as region, value as name from Input);

-- insert into Output
select
    region,
    name,
    sum(age) over w1 as sum,
    min(age) over w1 as min,
    max(age) over w1 as max,
    count(age) over w1 as count,
    count(*) over w1 as count_all,
    count_if(age>20) over w1 as count_if,
    some(age) over w1 as some,
    bit_and(age) over w1 as bit_and,
    bit_or(age) over w1 as bit_or,
    bit_xor(age) over w1 as bit_xor,
    bool_and(age>20) over w1 as bool_and,
    bool_or(age>20) over w1 as bool_or,
    avg(age) over w1 as avg,
    aggr_list(age) over w1 as `list`,
    min_by(age, name) over w1 as min_by,
    max_by(age, name) over w1 as max_by,
    nanvl(variance(age) over w1, -999.0) as variance,
    nanvl(stddev(age) over w1, -999.0) as stddev,
    nanvl(populationvariance(age) over w1, -999.0) as popvar,
    nanvl(stddevpopulation(age) over w1, -999.0) as popstddev,
    histogram(age) over w1 as hist,
    median(age) over w1 as median,
    percentile(age, 0.9) over w1 as perc90,
    aggregate_by(age, aggregation_factory("count")) over w1 as aggby
from $data
window w1 as (partition by region order by name desc)
order by region, name desc
;
