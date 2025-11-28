/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

SELECT
    Yql::SelectMembers(WithSideEffects(Ensure(<|a: 1|>, FALSE)), (AsAtom('b'),))
;
