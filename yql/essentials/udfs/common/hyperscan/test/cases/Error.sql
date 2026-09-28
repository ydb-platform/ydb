$input = AsList(
    <|value: ""|>,
    <|value: "a"|>,
    <|value: "aax"|>,
    <|value: "xaax"|>,
    <|value: "xaaxaaxaa"|>,
    <|value: "XAXA"|>,
    <|value: "7"|>,
    <|value: "QC transfer task JAVA"|>
);
PRAGMA config.flags("LLVM","OFF"); -- TODO: fix error handling with LLVM
$match = Hyperscan::Match("*");
SELECT $match(value) AS match FROM AS_TABLE($input);
