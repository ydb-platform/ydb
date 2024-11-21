/* syntax version 1 */
/* postgres can not */
select ListAggregate([],AGGREGATION_FACTORY("sum"));
select ListAggregate([1,2],AGGREGATION_FACTORY("sum"));
select ListAggregate(Just([1,2]),AGGREGATION_FACTORY("sum"));
select ListAggregate(null,AGGREGATION_FACTORY("sum"));
select DictAggregate({},AGGREGATION_FACTORY("sum"));
select DictAggregate({'a':[2,3]},AGGREGATION_FACTORY("sum"));
select DictAggregate(Just({'a':[2,3]}),AGGREGATION_FACTORY("sum"));
select DictAggregate(null,AGGREGATION_FACTORY("sum"));
