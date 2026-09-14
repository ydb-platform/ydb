$init = ($item, $parent) -> {
    RETURN Udf(String::AsciiToUpper, $parent AS Depends, 2 AS Depends)($item);
};

SELECT
    $init('foo', 1)
;
