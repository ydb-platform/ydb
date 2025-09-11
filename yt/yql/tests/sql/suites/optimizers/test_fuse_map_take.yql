/* postgres can not */
$data = (
	SELECT key AS Name, value AS Value FROM plato.Input0
);

$filtered = (
	SELECT * FROM $data WHERE Name != "BadName" LIMIT 10
);

SELECT Name, Avg(Length(Value)) AS Len FROM $filtered GROUP BY Name ORDER BY Name;
