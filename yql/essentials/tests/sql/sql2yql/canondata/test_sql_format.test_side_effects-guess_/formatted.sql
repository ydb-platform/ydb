/* custom error: Condition violated */
$vt = Variant<Int32, String>;

SELECT
    Variant(WithSideEffects(Ensure(1, FALSE)), '0', $vt).1
;
