/* postgres can not */
/* syntax version 1 */
SELECT
    ListNotNull(NULL),
    ListNotNull([]),
    ListNotNull([NULL]),
    ListNotNull(Just([])),
    ListNotNull([1, 2]),
    ListNotNull([1, NULL, 2]),
    ListNotNull(Just([1, 2])),
    ListNotNull(Just([1, NULL, 2])),
    ListNotNull(Nothing(List<Int32>?)),
    ListNotNull(Nothing(List<Int32?>?))
;
