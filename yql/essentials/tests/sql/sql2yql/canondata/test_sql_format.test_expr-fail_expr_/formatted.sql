/* custom error:Detected a type error after initial validation*/
PRAGMA warning('disable', '4510');

SELECT
    Yql::FailMe(AsAtom('expr'))
;
