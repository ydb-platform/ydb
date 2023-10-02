/* syntax version 1 */

$src = [
    <|x:1/0, y:2/0|>,
    <|x:1/0, y:1|>,
    <|x:1, y:1/0|>,
    <|x:2, y:2|>,
    <|x:3, y:3|>,
    <|x:4, y:4|>,
    <|x:5, y:5|>,
];

select * from as_table($src) where not (x < 3 or y > 3);
