/* syntax version 1 */
/* postgres can not */
$factory = AGGREGATION_FACTORY("sum");

select ListAggregate(ListCreate(Int32), $factory);
select ListAggregate(AsList(1, 2, 3), $factory);
select ListAggregate(Just(AsList(1, 2, 3)), $factory);
select ListAggregate(Nothing(ParseType("List<Int32>?")), $factory);

$factory = AGGREGATION_FACTORY("count");

select ListAggregate(ListCreate(Int32), $factory);
select ListAggregate(AsList(1, 2, 3), $factory);
select ListAggregate(Just(AsList(1, 2, 3)), $factory);
select ListAggregate(Nothing(ParseType("List<Int32>?")), $factory);

$factory = AGGREGATION_FACTORY("sum");

select ListSort(DictItems(DictAggregate(
    DictCreate(ParseType("String"), ParseType("List<Int32>"))
    , $factory)));

select ListSort(DictItems(DictAggregate(
    AsDict(
        AsTuple("foo", AsList(1, 3)), 
        AsTuple("bar", AsList(2))
    ), $factory)));


select ListSort(DictItems(DictAggregate(
    Just(AsDict(
        AsTuple("foo", AsList(1, 3)), 
        AsTuple("bar", AsList(2))
    )), $factory)));

select ListSort(DictItems(DictAggregate(
    Nothing(ParseType("Dict<String, List<Int32>>?"))
    , $factory)));

$factory = AGGREGATION_FACTORY("count");

select ListSort(DictItems(DictAggregate(
    DictCreate(ParseType("String"), ParseType("List<Int32>"))
    , $factory)));

select ListSort(DictItems(DictAggregate(
    AsDict(
        AsTuple("foo", AsList(1, 3)), 
        AsTuple("bar", AsList(2))
    ), $factory)));


select ListSort(DictItems(DictAggregate(
    Just(AsDict(
        AsTuple("foo", AsList(1, 3)), 
        AsTuple("bar", AsList(2))
    )), $factory)));

select ListSort(DictItems(DictAggregate(
    Nothing(ParseType("Dict<String, List<Int32>>?"))
    , $factory)));
