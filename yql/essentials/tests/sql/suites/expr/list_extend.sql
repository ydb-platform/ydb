/* postgres can not */
/* syntax version 1 */
PRAGMA warning("disable", "1107");
SELECT
    ListExtend([2u], [3s], [4l]),
    ListExtend(1, "String", 123, null),
    ListExtendStrict(1, "String", 123, null),
    ListExtend([3s], [4], Just([5l])),
    ListExtend([1u], [2u], Nothing(List<Int32>?)),
    ListExtendStrict([1u], [2u], Nothing(List<UInt32>?)),
    ListExtendStrict([1u], [2u], [4u, 3u]),
    ListExtendStrict([1u], Just([2u]), [], [5u, 6u]),
;
