/* postgres can not */
/* syntax version 1 */
$f = AggregationFactory(
       "UDAF",
       ($item, $_) -> ($item),
       ($state, $item, $_) -> ($state),
       null,
       ($state) -> ($state)
);
    
select aggregate_by(x,$f) over (order by x) from (values (1),(2),(3)) as a(x);