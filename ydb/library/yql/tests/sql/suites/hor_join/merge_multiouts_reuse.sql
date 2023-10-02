use plato;

/* syntax version 1 */
/* postgres can not */
$udfScript = @@
def MyFunc(list):
    return [(int(x.key) % 4, x) for x in list]
@@;

$record = (SELECT TableRow() FROM Input);
$recordType =TypeOf(Unwrap($record));

$udf = Python::MyFunc(
    CallableType(0,
        StreamType(
            VariantType(TupleType($recordType, $recordType, $recordType, $recordType))
        ),
        StreamType($recordType)),
    $udfScript
);

$i0, $i1, $i2, $i3 = (PROCESS Input USING $udf(TableRows()));

select * from (
    select * from $i0
    union all
    select * from $i1
    union all
    select * from $i2
    union all
    select * from $i0
) order by key;

insert into Output
select * from (
    select * from $i3
    union all
    select * from $i1
    union all
    select * from $i2
    union all
    select * from $i3
);
