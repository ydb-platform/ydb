/* syntax version 1 */
/* postgres can not */
$list = [AsVariant(1, "int"),AsVariant(1.f, "float"),AsVariant(1.0, "double"),AsVariant(1u, "unsigned")];
SELECT ListSortAsc($list) AS asc, ListSortDesc($list) AS desc;
