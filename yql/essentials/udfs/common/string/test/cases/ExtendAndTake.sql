$split = ($row) -> {
    return String::SplitToList($row.value, " ", true AS SkipEmpty, false AS DelimeterString);
};

$input = AsList(
    <|value:"a b c"|>,
    <|value:"d"|>,
    <|value:""|>
);

SELECT
    $split(TableRow()),
    ListExtend($split(TableRow()), $split(TableRow()))[1]
FROM AS_TABLE($input);
