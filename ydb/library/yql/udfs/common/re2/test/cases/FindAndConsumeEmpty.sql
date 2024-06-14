/* syntax version 1 */
$regexp = Re2::FindAndConsume("(a*)");

SELECT $regexp("abaa");