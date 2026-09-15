$do_safe_cast = ($x, $type) -> {
    $type_code = AtomCode($type);
    $cast_code = EvaluateCode(LambdaCode(($x_code) -> {
        RETURN FuncCode("SafeCast", $x_code, $type_code);
    }));
     RETURN $cast_code($x);
};

$do_safe_cast_2 = ($x, $type) -> ($do_safe_cast($x, FormatType($type)));

select $do_safe_cast_2(123, String);
