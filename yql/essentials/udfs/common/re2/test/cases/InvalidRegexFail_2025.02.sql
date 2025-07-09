/* syntax version 1 */
$invalidRe = Re2::FindAndConsume("[");

SELECT $invalidRe("abaa");
