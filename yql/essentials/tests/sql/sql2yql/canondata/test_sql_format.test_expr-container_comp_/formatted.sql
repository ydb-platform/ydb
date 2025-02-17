/* postgres can not */
SELECT
    AsStruct() == AsStruct()
;

SELECT
    AsTuple() == AsTuple()
;

SELECT
    AsTuple(0xffffffffu) == AsTuple(-1)
;

SELECT
    AsTuple(1, 2 / 0) < AsTuple(10, 1)
;

SELECT
    AsStruct(1 AS a, 2 AS b) == AsStruct(1u AS a, 2u AS b)
;

SELECT
    AsStruct(1 AS a, 2 AS b) == AsStruct(1u AS a, 2u AS c)
;

SELECT
    AsTuple(Void()) <= AsTuple(Void())
;

SELECT
    AsTuple(NULL) <= AsTuple(NULL)
;

SELECT
    AsTagged(1, 'foo') == AsTagged(1u, 'foo')
;
