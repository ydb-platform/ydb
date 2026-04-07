/* custom error: Linear types can be used */
$x = MutDictHasItems(MutDictCreate(Int32, String, 0));

SELECT
    WithSideEffects(($x, 1)).1
;
