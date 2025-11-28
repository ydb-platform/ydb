$init = ($item, $parent) -> {
    RETURN Udf(String::AsciiToUpper, $parent AS Depends)($item);
};

SELECT
    $init('foo', 1)
;
