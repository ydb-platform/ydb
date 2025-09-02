/* custom error: Condition violated */
PRAGMA warning('disable', '4510');

SELECT
    Yql::SelectMembers(Yql::WithSideEffectsMode(Ensure(<|a: 1|>, FALSE), AsAtom('General')), (AsAtom('b'),))
;
