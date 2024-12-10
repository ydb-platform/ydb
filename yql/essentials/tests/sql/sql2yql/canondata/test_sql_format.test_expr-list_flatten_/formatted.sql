/* postgres can not */
/* syntax version 1 */
SELECT
    ListFlatten(NULL),
    ListFlatten([]),
    ListFlatten(Just([])),
    ListFlatten(Nothing(EmptyList?)),
    ListFlatten([[], []]),
    ListFlatten([NULL, NULL]),
    ListFlatten([Just([]), Just([])]),
    ListFlatten(Just([[], []])),
    ListFlatten(Just([NULL, NULL])),
    ListFlatten(Just([Just([]), Just([])]))
;

SELECT
    ListFlatten([[1, 2], [3, 4]]),
    ListFlatten([[1, 2], NULL, [3, 4]]),
    ListFlatten(Just([[1, 2], [3, 4]])),
    ListFlatten(Just([[1, 2], NULL, [3, 4]])),
    ListFlatten([Just([1, 2]), Just([3, 4])]),
    ListFlatten(Just([Just([1, 2]), Just([3, 4])]))
;
