use plato;

$udf = YQL::@@(lambda '(flow) 
(Map flow
   (lambda '(item) (block '(
        (let varTuple (VariantType (TupleType
            (StructType
                '('key (DataType 'String))
                '('subkey (DataType 'String))
                '('value (DataType 'String))
            )
            (StructType
                '('key (DataType 'String))
                '('subkey (DataType 'String))
                '('value (DataType 'String))
            )
        )))
        (let intValue (FromString (Member item 'key) 'Int32))
        (let res
            (If (Coalesce (Equal (% intValue (Int32 '2)) (Int32 '0)) (Bool 'false))
                (Variant item '0 varTuple)
                (Variant item '1 varTuple)
            )
        )
        (return res)
    )))
))@@;

$i, $j = (PROCESS Input USING $udf(TableRows()));

select key, value from $i where key > "100"
order by key;

insert into @a
select * from $j;

insert into @b
select key from $i where key > "200";

insert into @c
select key from $j where key > "300";
