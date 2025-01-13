/* syntax version 1 */
DECLARE $x AS Struct<a: Int64, b: String?>;

SELECT
    CAST($x.a AS String) || coalesce($x.b, 'zzz')
;
