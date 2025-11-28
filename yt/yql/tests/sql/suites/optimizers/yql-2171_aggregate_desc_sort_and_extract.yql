/* postgres can not */
/* syntax version 1 */
USE plato;

$data = (select cast(key as uint32) as age, cast(subkey as uint32) as region, value as name from Input);

$top_users_by_age_dec = (SELECT
    age_dec,
    COUNT(1) as age_dec_count
FROM $data
GROUP BY age / 10 as age_dec
ORDER BY age_dec_count DESC
LIMIT 2);

--INSERT INTO Output
SELECT age_dec, info.* FROM $top_users_by_age_dec AS top JOIN $data AS info ON top.age_dec = info.age / 10 ORDER BY name;
