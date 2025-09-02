/* syntax version 1 */
$regexp1 = Re2::FindAndConsume("(a*)");
$regexp2 = Re2::FindAndConsume("a(b*)");

SELECT $regexp1("abaa");
SELECT $regexp2("a");