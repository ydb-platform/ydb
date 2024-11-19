SELECT SUM(CAST(subkey AS Uint8)) AS sum
FROM plato.Input5
GROUP BY CAST(CAST(key AS Uint16) AS TzDate);
