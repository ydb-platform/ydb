/* syntax version 1 */
/* postgres can not */
$f = ($x,$y?)->($x + ($y ?? 0));
select ListMap([1,2,3],$f);

$g = ($x,$y,$z?)->(($x + $y + $z) ?? 100);
select Yql::Fold([1,2,3],0,$g);
