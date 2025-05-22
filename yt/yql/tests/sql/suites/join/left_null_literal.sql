/* syntax version 1 */
use plato;

$foo = [<|"x":1|>];
$bar = [<|"x":1, "y":Null|>];

insert into Output
select * from AS_TABLE($foo) as a LEFT JOIN AS_TABLE($bar) as b USING(x);
