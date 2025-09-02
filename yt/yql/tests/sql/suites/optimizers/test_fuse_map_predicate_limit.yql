/* postgres can not */

$data = (
	SELECT key AS Key, YQL::Substring(key, 1, 1) AS Category FROM plato.Input0 WHERE length(key) > 2 LIMIT 20
);

SELECT
    Category,
    COUNT(*)
FROM $data
GROUP BY Category
ORDER BY Category ASC;