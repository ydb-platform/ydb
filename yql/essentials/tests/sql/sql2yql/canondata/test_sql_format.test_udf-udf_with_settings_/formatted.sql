$g = String::AsciiToUpper;
$g = Udf($g, '64.0' AS Cpu, '4294967296' AS ExtraMem);

SELECT
    $g('foo')
;
