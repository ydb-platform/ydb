/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

$vt = Variant<Int32, String>;

SELECT
    Way(Variant(Yql::WithSideEffectsMode(Ensure(1, FALSE), AsAtom('General')), '0', $vt))
;
