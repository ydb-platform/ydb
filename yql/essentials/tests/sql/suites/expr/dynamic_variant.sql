$dt = Int32;
$tvt = Variant<$dt,$dt>;
select ListMap([(10,0us),(20,2us)],($x)->(DynamicVariant($x.0,$x.1,$tvt)));

$dt = Int32;
$svt = Variant<x:$dt,y:$dt>;
select ListMap([(10,'x'),(20,'z'),(30,null)],($x)->(DynamicVariant($x.0,cast($x.1 as utf8),$svt)));
