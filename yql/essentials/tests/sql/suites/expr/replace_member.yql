/* postgres can not */
/* syntax version 1 */
$data = <|x: [<|y: 2|>], z: 5|>;

-- set field function
$F = ($field, $function) -> (
    ($struct) -> (
        ReplaceMember($struct, $field, $function($struct.$field))
    )
);

-- set list element function
$E = ($index, $function) -> (
    ($list) -> (
        ListMap(ListEnumerate($list), ($pair) -> (
            IF ($pair.0 = $index, $function($pair.1), $pair.1)
        ))
    )
);

-- set value function
$V = ($value) -> (
    ($_item) -> ($value)
);

SELECT $F("x", $E(0, $F("y", $V(3))))($data)

