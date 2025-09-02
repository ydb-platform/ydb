/* syntax version 1 */
$vt1 = Variant<Int32,String>;
select TOP(x,3) from 
(select [Variant(1,"0",$vt1),Variant("str","1",$vt1),Variant(1,"0",$vt1)] as x)
flatten list by x;
