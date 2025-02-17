/* syntax version 1 */
PRAGMA config.flags("LLVM","OFF"); -- TODO: fix error handling with LLVM
$match = Hyperscan::Match("*");
SELECT $match(value) AS match FROM Input;
