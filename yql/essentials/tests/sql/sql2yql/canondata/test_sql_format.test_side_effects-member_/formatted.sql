/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

SELECT
    <|a: 1, b: Yql::WithSideEffectsMode(Ensure(2, FALSE), AsAtom('General'))|>.a
;
