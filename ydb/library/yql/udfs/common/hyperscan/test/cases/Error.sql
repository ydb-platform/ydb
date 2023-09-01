/* syntax version 1 */
$match = Hyperscan::Match("*");
--PRAGMA config.flags("LLVM","OFF");
SELECT $match(value) AS match FROM Input;
