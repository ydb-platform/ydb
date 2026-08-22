/* custom error:too much iterations*/
PRAGMA warning('disable', '4510');
PRAGMA config.flags('RepeatTransformLimit', '1000');
PRAGMA config.flags('TransformCycleDetector', '4');

SELECT
    Yql::FailMe(AsAtom('opt_cycle'))
;
