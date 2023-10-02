/* postgres can not */
/* syntax version 1 */
select ListFlatMap([1,2,null],($x)->(10 + $x));
