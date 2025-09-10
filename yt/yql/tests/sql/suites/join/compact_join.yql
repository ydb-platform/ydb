USE plato;

$stream = SELECT a.k AS k, a.sk AS ask, a.v AS av
FROM InputLeft AS a
INNER JOIN /*+ merge() compact() */ InputRight AS b
USING (k, sk, v);

SELECT k, ask, av
FROM $stream
GROUP /*+ compact() */ BY (k, ask, av)
ORDER BY k, ask, av;