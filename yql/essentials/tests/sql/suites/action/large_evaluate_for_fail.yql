/* syntax version 1 */
/* postgres can not */
/* custom error: Too large list for EVALUATE FOR, allowed: 3, got: 10 */
pragma config.flags("EvaluateForLimit", "3");

evaluate for $_i in ListFromRange(0, 10) do empty_action();
