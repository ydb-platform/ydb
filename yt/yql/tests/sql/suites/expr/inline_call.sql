/* postgres can not */
/* syntax version 1 */
use plato;

$x = AsStruct(1 as a);
select $x.a;

$y = AsTuple(2,3);
select $y.1;

select length("foo");

select Math::Pi();

$f = () -> { 
    return () -> {
        return AsDict(AsTuple("foo",AsList(AsStruct(AsTuple(1) as bar))));
    }
};

select $f()()["foo"][0].bar.0;

select ()->{return 1}();

$type = Callable<()->List<Int32>>;
$g = AsStruct(Yql::Callable($type, ()->{return AsList(1,2,3)}) as foo);
    
select $g.foo()[0];
