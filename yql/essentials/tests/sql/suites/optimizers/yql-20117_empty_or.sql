pragma config.flags("OptimizerFlags","ExtractCommonPredicatesFromLogicalOps");

$between_5_and_10 = ($x) -> ($x >= 5 and $x <= 10 or $x <= 10 and $x >= 5);

select x, $between_5_and_10(x) from as_table([<|x:1|>, <|x:7|>]);
