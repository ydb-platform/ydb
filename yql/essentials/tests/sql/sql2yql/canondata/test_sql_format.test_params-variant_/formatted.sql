/* syntax version 1 */
-- underlying type is tuple
DECLARE $x1 AS Variant<String, Int64>;
DECLARE $x2 AS Variant<String, Int64>;

-- underlying type is struct
DECLARE $x3 AS Variant<a: String, b: Int64>;
DECLARE $x4 AS Variant<a: String, b: Int64>;
DECLARE $x5 AS Variant<a: String, b: Int64>;

SELECT
    $x1.0 || CAST($x2.1 AS String) || $x3.a || CAST($x4.b AS String) || $x5.a
;
