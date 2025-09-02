/* syntax version 1 */

$split = ($row) -> {
    return String::SplitToList($row.value, " ", true AS SkipEmpty, false AS DelimeterString);
};

SELECT
    $split(TableRow()),
    ListExtend($split(TableRow()), $split(TableRow()))[1]
FROM Input;
