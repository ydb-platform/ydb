/* custom error:Graph cycle detected*/
PRAGMA warning('disable', '4510');
PRAGMA config.flags('RepeatTransformLimit', '1000');
PRAGMA config.flags('TransformCycleDetector', '10');

SELECT
    Yql::FailMe(AsAtom('opt_cycle'))
;
