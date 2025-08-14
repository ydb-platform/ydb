PRAGMA warning('disable', '4510');

SELECT
    Yql::WithSideEffectsMode(1, AsAtom('SemilatticeRT'))
    + Yql::WithSideEffectsMode(1, AsAtom('SemilatticeRT'))
;
