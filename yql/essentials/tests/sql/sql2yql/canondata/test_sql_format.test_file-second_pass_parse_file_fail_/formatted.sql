/* postgres can not */
/* custom error:The file iterator was already created. To scan file data multiple times please use ListCollect either over ParseFile or over some lazy function over it, e.g. ListMap*/
$list = ParseFile('int32', 'keyid.lst');

SELECT
    ListExtend(
        ListMap(
            $list, ($x) -> {
                RETURN $x + 1;
            }
        ),
        ListMap(
            $list, ($x) -> {
                RETURN $x + 2;
            }
        )
    )
;
