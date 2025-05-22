/* postgres can not */
$null_t = TypeOf(NULL);
$struct_t = Struct<a: String, b: Int32?, c: $null_t, d: pgtext>;

$callable = Callable(
    Callable<($struct_t) -> $struct_t>, ($x) -> {
        RETURN $x;
    }
);

SELECT
    $callable(<|a: '1'|>),
    $callable(AddMember(<||>, 'a', '2'))
;
