/* syntax version 1 */
/* postgres can not */
pragma config.flags("EvaluateForLimit", "3");

evaluate for $_i in ListFromRange(0, 10) do empty_action();
