/* postgres can not */
USE plato;
pragma yt.UseNativeDescSort;

$udf = YQL::@@(lambda '(key stream) (AsStruct
  '('key key) '('summ (Collect (Condense stream (Nothing (OptionalType (DataType 'String))) (lambda '(item state) (Bool 'False)) (lambda '(item state) (Coalesce state (Just item))))))
))@@;

select * from (
    reduce Input1 presort value desc on key, subkey using $udf(value) --YtReduce
) order by key, summ;

select * from (
    reduce Input1 presort subkey desc, value desc on key using $udf(value) --YtReduce
) order by key, summ;

select * from (
    reduce Input1 presort value on key, subkey using $udf(value) --YtMapReduce
) order by key, summ;

select * from (
    reduce concat(Input1, Input2) presort value desc on key, subkey using $udf(value) --YtMapReduce
) order by key, summ;
