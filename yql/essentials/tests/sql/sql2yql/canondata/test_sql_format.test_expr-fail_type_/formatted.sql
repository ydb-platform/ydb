/* custom error:Rewrite error, type should be : String, but it is: Int32 for node Int32*/
PRAGMA warning('disable', '4510');

SELECT
    Yql::FailMe(AsAtom('type'))
;
