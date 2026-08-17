/* custom error: Bad extraMem setting value: invalid. Expected a number (bytes) or a human-readable size like '2048M', '1G', '512K'*/
$g_invalid = String::AsciiToUpper;
$g_invalid = Udf($g_invalid, 'invalid' AS ExtraMem);

SELECT
    $g_invalid('test') AS result
;
