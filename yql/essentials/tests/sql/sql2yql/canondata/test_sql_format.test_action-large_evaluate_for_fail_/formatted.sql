/* syntax version 1 */
/* postgres can not */
/* custom error: Too large list for EVALUATE FOR, allowed: 3, got: 10 */
PRAGMA config.flags('EvaluateForLimit', '3');

EVALUATE FOR $_i IN ListFromRange(0, 10) DO
    EMPTY_ACTION()
;
