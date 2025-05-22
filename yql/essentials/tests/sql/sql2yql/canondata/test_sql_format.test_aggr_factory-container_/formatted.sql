/* syntax version 1 */
/* postgres can not */
$factory = AGGREGATION_FACTORY('sum');

SELECT
    ListAggregate(ListCreate(Int32), $factory)
;

SELECT
    ListAggregate(AsList(1, 2, 3), $factory)
;

SELECT
    ListAggregate(Just(AsList(1, 2, 3)), $factory)
;

SELECT
    ListAggregate(Nothing(ParseType('List<Int32>?')), $factory)
;

$factory = AGGREGATION_FACTORY('count');

SELECT
    ListAggregate(ListCreate(Int32), $factory)
;

SELECT
    ListAggregate(AsList(1, 2, 3), $factory)
;

SELECT
    ListAggregate(Just(AsList(1, 2, 3)), $factory)
;

SELECT
    ListAggregate(Nothing(ParseType('List<Int32>?')), $factory)
;

$factory = AGGREGATION_FACTORY('sum');

SELECT
    ListSort(
        DictItems(
            DictAggregate(
                DictCreate(ParseType('String'), ParseType('List<Int32>')),
                $factory
            )
        )
    )
;

SELECT
    ListSort(
        DictItems(
            DictAggregate(
                AsDict(
                    AsTuple('foo', AsList(1, 3)),
                    AsTuple('bar', AsList(2))
                ), $factory
            )
        )
    )
;

SELECT
    ListSort(
        DictItems(
            DictAggregate(
                Just(
                    AsDict(
                        AsTuple('foo', AsList(1, 3)),
                        AsTuple('bar', AsList(2))
                    )
                ), $factory
            )
        )
    )
;

SELECT
    ListSort(
        DictItems(
            DictAggregate(
                Nothing(ParseType('Dict<String, List<Int32>>?')),
                $factory
            )
        )
    )
;

$factory = AGGREGATION_FACTORY('count');

SELECT
    ListSort(
        DictItems(
            DictAggregate(
                DictCreate(ParseType('String'), ParseType('List<Int32>')),
                $factory
            )
        )
    )
;

SELECT
    ListSort(
        DictItems(
            DictAggregate(
                AsDict(
                    AsTuple('foo', AsList(1, 3)),
                    AsTuple('bar', AsList(2))
                ), $factory
            )
        )
    )
;

SELECT
    ListSort(
        DictItems(
            DictAggregate(
                Just(
                    AsDict(
                        AsTuple('foo', AsList(1, 3)),
                        AsTuple('bar', AsList(2))
                    )
                ), $factory
            )
        )
    )
;

SELECT
    ListSort(
        DictItems(
            DictAggregate(
                Nothing(ParseType('Dict<String, List<Int32>>?')),
                $factory
            )
        )
    )
;
