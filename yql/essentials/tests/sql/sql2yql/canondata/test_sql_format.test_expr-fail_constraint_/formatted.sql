/* custom error:missing Empty constraint in node AsList*/
PRAGMA warning('disable', '4510');

SELECT
    Yql::FailMe(AsAtom('constraint'))
;
