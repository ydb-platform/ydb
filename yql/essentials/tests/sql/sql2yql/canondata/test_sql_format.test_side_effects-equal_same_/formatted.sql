/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

$t = Yql::WithSideEffectsMode(Ensure(TRUE, FALSE), AsAtom('General'));

SELECT
    $t == $t
;
