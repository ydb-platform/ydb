/* syntax version 1 */
DECLARE $x AS Uint32?;

SELECT
    2 * coalesce($x, 33)
;
