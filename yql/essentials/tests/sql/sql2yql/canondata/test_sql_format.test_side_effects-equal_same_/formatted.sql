/* custom error: Condition violated */
$t = WithSideEffects(Ensure(TRUE, FALSE));

SELECT
    $t == $t
;
