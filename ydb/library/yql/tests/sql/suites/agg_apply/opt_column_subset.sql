/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

$x = select * from as_table([<|x:1,y:2,z:3|>, <|x:4,y:5,z:6|>]);
select count(x + y) from $x;
