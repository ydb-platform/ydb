USE plato;

pragma DqEngine = "disable"; 

DEFINE SUBQUERY $sample($product_type) AS

    SELECT *
    FROM Input
    WHERE subkey = $product_type
    ORDER BY key
    LIMIT 10;

END DEFINE;

$list = ["a", "b"];
$s = SubqueryUnionAllFor($list, $sample);
$concated = PROCESS $s();

INSERT INTO Output 
SELECT *
FROM $concated