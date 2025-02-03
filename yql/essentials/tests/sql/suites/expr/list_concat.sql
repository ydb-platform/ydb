/* postgres can not */
/* syntax version 1 */
$list = ["one","two","three","four","five"];

SELECT
    ListConcat([], "."),
    ListConcat($list),
    ListConcat($list, ";"),
    ListConcat($list, Just(", ")),
    ListConcat($list, NULL),
    ListConcat($list, Nothing(String?)),
    ListConcat(["single"], "tail");
