/* syntax version 1 */
$tag1 = "ta" || "g1";
$struct = struct< $tag1 : string, tag2 : string, "tag3" : string >;

$type1 = TypeOf(10);

select FormatType(list<$type1>), FormatType($struct);

