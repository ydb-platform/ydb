/* postgres can not */
$d2 = AsDict(AsTuple(1 / 1, 'bar'));
$d3 = AsDict(AsTuple(1 / 0, 'baz'));
$t1 = AsDict(AsTuple(AsTuple(1, 'key'), AsTuple(1, 'value')));
$t2 = AsDict(AsTuple(AsTuple(1 / 1, 'key'), AsTuple(2 / 1, 'value')));
$t3 = AsDict(AsTuple(AsTuple(1 / 0, 'key'), AsTuple(123, 'value')));

SELECT
    DictContains($d2, NULL), -- false, no such key
    DictContains($d3, NULL), -- true, null is convertible to Nothing<T> for any T
    DictLookup($d2, NULL), -- Nothing(String?), no such key
    DictLookup($d3, NULL)
; -- Just("baz"), null is convertible to Nothing<T> for any T

SELECT
    DictContains($t1, AsTuple(1, 'keyy')), -- false, missing key
    DictContains($t1, AsTuple(1, 'key')), -- true,  match
    DictContains($t1, Just(AsTuple(1, 'key'))), -- true,  match with optional
    DictContains($t2, AsTuple(NULL, 'key')), -- false, no such key
    DictContains($t3, AsTuple(NULL, 'key')), -- true, null is convertible to Nothing<T> for any T
    DictLookup($t2, AsTuple(NULL, 'key')), -- Nothing(Tuple<Int32?, String>?), no such key
    DictLookup($t3, AsTuple(NULL, 'key'))
; -- Just(AsTuple(123, "value")), null is convertible to Nothing<T> for any T
