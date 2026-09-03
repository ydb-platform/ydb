$g1 = String::AsciiToUpper;
$g1 = Udf($g1, '2147483648' AS ExtraMem);

$g2 = String::AsciiToUpper;
$g2 = Udf($g2, '1G' AS ExtraMem);

$g3 = String::AsciiToUpper;
$g3 = Udf($g3, '512M' AS ExtraMem);

$g4 = String::AsciiToUpper;
$g4 = Udf($g4, '1T' AS ExtraMem);

$g5 = String::AsciiToUpper;
$g5 = Udf($g5, '512K' AS ExtraMem);

SELECT
    $g1('test1') AS r1,
    $g2('test2') AS r2,
    $g3('test3') AS r3,
    $g4('test4') AS r4,
    $g5('test5') AS r5
;
