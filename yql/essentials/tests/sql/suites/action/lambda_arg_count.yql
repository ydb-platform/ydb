/* syntax version 1 */
/* postgres can not */
SELECT
   LambdaArgumentsCount(()->(1)),
   LambdaArgumentsCount(($x)->($x+1)),
   LambdaArgumentsCount(($x, $y)->($x+$y)),
   LambdaArgumentsCount(($x?)->($x+1)),
   LambdaArgumentsCount(($x, $y?)->($x+$y)),

   LambdaOptionalArgumentsCount(()->(1)),
   LambdaOptionalArgumentsCount(($x)->($x+1)),
   LambdaOptionalArgumentsCount(($x, $y)->($x+$y)),
   LambdaOptionalArgumentsCount(($x?)->($x+1)),
   LambdaOptionalArgumentsCount(($x, $y?)->($x+$y));
