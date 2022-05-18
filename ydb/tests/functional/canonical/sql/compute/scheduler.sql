SELECT id, SUM(nodes) AS nodes FROM (
    SELECT id AS id, 0 AS nodes FROM schedulers
    UNION ALL SELECT scheduler AS id, COUNT(*) AS nodes FROM compute_nodes
    GROUP BY scheduler)
GROUP BY id
ORDER BY id;

