USE plato;

insert into @skv1v2
select key, subkey, value as value1, value as value2 from Input order by subkey, key, value1, value2;

insert into @skv2v1
select key, subkey, value as value1, value as value2 from Input order by subkey, key, value2, value1;

insert into @ksv1v2
select key, subkey, value as value1, value as value2 from Input order by key, subkey, value1, value2;

commit;

$udf = YQL::@@(lambda '(key stream) (AsStruct
  '('key key) '('summ (Collect (Condense stream (Nothing (OptionalType (DataType 'String))) (lambda '(item state) (Bool 'False)) (lambda '(item state) (Coalesce state (Just item))))))
))@@;

select * from (
    reduce concat(@skv1v2, @skv1v2) presort value1, value2 on key, subkey using $udf(value1) --YtReduce
) order by key, summ;

select * from (
    reduce @ksv1v2 presort value2, value1 on key, subkey using $udf(value1) --YtMapReduce
) order by key, summ;

select * from (
    reduce concat(@skv1v2, @skv2v1) presort value1, value2 on key, subkey using $udf(value1) --YtMapReduce
) order by key, summ;

select * from (
    reduce concat(@skv1v2, @ksv1v2) presort value1, value2 on key, subkey using $udf(value1) --YtMapReduce
) order by key, summ;
