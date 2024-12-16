$vartype = Variant<Int32, Bool, String>;

$handle_num = ($x) -> {
    RETURN 2 * $x;
};

$handle_flag = ($x) -> {
    RETURN If($x, 200, 10);
};

$handle_str = ($x) -> {
    RETURN Unwrap(CAST(LENGTH($x) AS Int32));
};

$visitor = ($var) -> {
    RETURN Visit($var, $handle_num, $handle_flag, $handle_str);
};

SELECT
    $visitor(VARIANT (5, '0', $vartype)),
    $visitor(Just(VARIANT (TRUE, '1', $vartype))),
    $visitor(Just(VARIANT ('somestr', '2', $vartype))),
    $visitor(Nothing(OptionalType($vartype))),
    $visitor(NULL)
;

$visitor_def = ($var) -> {
    RETURN VisitOrDefault($var, 999, $handle_num, $handle_flag);
};

SELECT
    $visitor_def(VARIANT (5, '0', $vartype)),
    $visitor_def(Just(VARIANT (TRUE, '1', $vartype))),
    $visitor_def(Just(VARIANT ('somestr', '2', $vartype))),
    $visitor_def(Nothing(OptionalType($vartype))),
    $visitor_def(NULL)
;

$vartype1 = Variant<Int32, Int32, Int32>;

SELECT
    VariantItem(VARIANT (7, '1', $vartype1)),
    VariantItem(Just(VARIANT (5, '0', $vartype1))),
    VariantItem(Nothing(OptionalType($vartype1))),
    VariantItem(NULL)
;
