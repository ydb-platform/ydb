$f = block(
    ($arg) -> {
        $x = MutDictCreate(int32, string, $arg, 0);
        $y = MutDictCreate(int32, string, $arg, 1);
        $swap = Opaque(TRUE);
        $A, $B = IF($swap, ($x, $y), ($y, $x));
        RETURN (FromMutDict($A), FromMutDict($B));
    }
);

SELECT
    $f
;
