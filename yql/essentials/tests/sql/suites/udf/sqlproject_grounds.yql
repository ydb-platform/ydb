/* syntax version 1 */
/* postgres can not */

$ts_bytes = String::HexDecode('0a0b0c0d');
$ts_int = FromBytes($ts_bytes, Uint32);
SELECT 
    $ts_int,
    DateTime::FromSeconds($ts_int)
