/* syntax version 1 */
/* postgres can not */
SELECT
    ListAggregate([], AGGREGATION_FACTORY('sum'))
;

SELECT
    ListAggregate([1, 2], AGGREGATION_FACTORY('sum'))
;

SELECT
    ListAggregate(Just([1, 2]), AGGREGATION_FACTORY('sum'))
;

SELECT
    ListAggregate(NULL, AGGREGATION_FACTORY('sum'))
;

SELECT
    DictAggregate({}, AGGREGATION_FACTORY('sum'))
;

SELECT
    DictAggregate({'a': [2, 3]}, AGGREGATION_FACTORY('sum'))
;

SELECT
    DictAggregate(Just({'a': [2, 3]}), AGGREGATION_FACTORY('sum'))
;

SELECT
    DictAggregate(NULL, AGGREGATION_FACTORY('sum'))
;
