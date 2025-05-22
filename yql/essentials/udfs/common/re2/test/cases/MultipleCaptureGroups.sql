/* syntax version 1 */
$regexp = Re2::Capture("(?P<groupname1>a)(?P<groupname2>b)(?<groupname1>c)");

select $regexp("abc");