/* syntax version 1 */
/* postgres can not */
use plato;

DEFINE ACTION $aaa($z) as 

$table = $z.0;
$k = (select min(key || $z.1) from $table);

DEFINE ACTION $bbb($n) AS
   SELECT $n || $k FROM $table;
END DEFINE;

$ccc = EvaluateCode(QuoteCode($bbb));
DO $ccc("1");

END DEFINE;

EVALUATE FOR $z IN AsList(AsTuple("Input","foo"),AsTuple("Input","bar")) 
    DO $aaa($z);
