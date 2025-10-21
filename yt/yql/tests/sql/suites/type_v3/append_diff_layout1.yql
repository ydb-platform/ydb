/* multirun can not */
/* postgres can not */
use plato;

pragma yt.UseNativeYtTypes="1";
pragma yt.NativeYtTypeCompatibility="complex";

$udf = YQL::@@(lambda '(x) (block '(
    (let structType (StructType '('key (DataType 'String)) '('subkey (StructType '('a (DataType 'String)) '('b (OptionalType (DataType 'Int32))) '('c (DataType 'String))))))
    (let varType (VariantType (TupleType structType structType structType)))
    (let res (Map x
        (lambda '(r)
            (If
                (Coalesce (> (SafeCast (Member r 'key) (DataType 'Int32)) (Int32 '200)) (Bool 'false))
                (Variant r '0 varType)
                (If
                    (Coalesce (< (SafeCast (Member r 'key) (DataType 'Int32)) (Int32 '50)) (Bool 'false))
                    (Variant r '1 varType)
                    (Variant r '2 varType)
                )
            )
        )
    ))
    (return res)
)))@@;

$i, $j, $k = (PROCESS Input USING $udf(TableRows()));

insert into Output1
select *
from $i;

insert into Output2
select *
from $j
limit 2;

insert into Output3
select *
from $k
order by key;
