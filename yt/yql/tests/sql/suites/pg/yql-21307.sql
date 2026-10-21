--!syntax_pg
/* hybridfile can not */
/* dqfile can not */

WITH RECURSIVE
    x AS (SELECT key FROM plato."Input"),
    r AS (
        SELECT key FROM x
        UNION ALL
        SELECT key FROM r WHERE FALSE
    )
SELECT Count(key) FROM r;
