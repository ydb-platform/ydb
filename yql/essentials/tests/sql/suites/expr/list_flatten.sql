/* postgres can not */
/* syntax version 1 */
select ListFlatten(null),ListFlatten([]),
    ListFlatten(Just([])),ListFlatten(Nothing(EmptyList?)),
    ListFlatten([[],[]]),ListFlatten([null,null]),
    ListFlatten([Just([]),Just([])]),
    ListFlatten(Just([[],[]])),ListFlatten(Just([null,null])),
    ListFlatten(Just([Just([]),Just([])]));
    
select ListFlatten([[1,2],[3,4]]),
    ListFlatten([[1,2],null,[3,4]]),
    ListFlatten(Just([[1,2],[3,4]])),
    ListFlatten(Just([[1,2],null,[3,4]])),
    ListFlatten([Just([1,2]),Just([3,4])]),
    ListFlatten(Just([Just([1,2]),Just([3,4])]));