/* syntax version 1 */
$tag1 = 'ta' || 'g1';
$struct = struct<$tag1: string, tag2: string, 'tag3': string, tag4: Null>;

$type1 = TypeOf(10);

SELECT
    FormatType(list<$type1>),
    FormatType($struct)
;
