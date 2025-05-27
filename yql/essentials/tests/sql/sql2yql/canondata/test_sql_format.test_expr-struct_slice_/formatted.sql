/* postgres can not */
/* syntax version 1 */
$s = <|a: 1, b: 2, c: 3, d: 4|>;
$list = ['a', 'b'];

SELECT
    *
FROM (
    SELECT
        CombineMembers(
            ChooseMembers($s, $list),
            <|remain: RemoveMembers($s, $list)|>
        )
)
    FLATTEN COLUMNS
;
