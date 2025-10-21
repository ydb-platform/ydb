$vartype = Variant<Int32, Bool, String>;
$handle_num = ($x) -> { return 2 * $x; };
$handle_flag = ($x) -> { return If($x, 200, 10); };
$handle_str = ($x) -> { return Unwrap(CAST(LENGTH($x) AS Int32)); };

$visitor = ($var) -> { return Visit($var, $handle_num, $handle_flag, $handle_str); };
SELECT
    $visitor(Variant(5, "0", $vartype)),
    $visitor(Just(Variant(True, "1", $vartype))),
    $visitor(Just(Variant("somestr", "2", $vartype))),
    $visitor(Nothing(OptionalType($vartype))),
    $visitor(NULL)
;

$visitor_def = ($var) -> { return VisitOrDefault($var, 999, $handle_num, $handle_flag); };
SELECT
    $visitor_def(Variant(5, "0", $vartype)),
    $visitor_def(Just(Variant(True, "1", $vartype))),
    $visitor_def(Just(Variant("somestr", "2", $vartype))),
    $visitor_def(Nothing(OptionalType($vartype))),
    $visitor_def(NULL)
;

$vartype1 = Variant<Int32, Int32, Int32>;
SELECT
    VariantItem(Variant(7, "1", $vartype1)),
    VariantItem(Just(Variant(5, "0", $vartype1))),
    VariantItem(Nothing(OptionalType($vartype1))),
    VariantItem(NULL)
;