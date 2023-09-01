/* syntax version 1 */
$vt1 = Variant<Int32,String>;
select AGGREGATE_LIST_DISTINCT(x) from 
(select [Variant(1,"0",$vt1),Variant("str","1",$vt1),Variant(1,"0",$vt1)] as x)
flatten list by x;

$vt2 = Variant<x:Int32,y:String>;
select AGGREGATE_LIST_DISTINCT(x) from 
(select [Variant(1,"x",$vt2),Variant("str","y",$vt2),Variant(1,"x",$vt2)] as x)
flatten list by x;

