$vartype = Variant<num: Int32, flag: Bool, str: String>;
$handle_num = ($x) -> { return 2 * $x; };
$handle_flag = ($x) -> { return If($x, 200, 10); };
$handle_str = ($x) -> { return Unwrap(CAST(LENGTH($x) AS Int32)); };

$visitor = ($var) -> { return Visit($var, $handle_num AS num, $handle_flag AS flag, $handle_str AS str); };
SELECT
    $visitor(Variant(5, "num", $vartype)),
    $visitor(Just(Variant(True, "flag", $vartype))),
    $visitor(Just(Variant("somestr", "str", $vartype))),
    $visitor(Nothing(OptionalType($vartype))),
    $visitor(NULL)
;

$visitor_def = ($var) -> { return VisitOrDefault($var, 999, $handle_num AS num, $handle_flag AS flag); };
SELECT
    $visitor_def(Variant(5, "num", $vartype)),
    $visitor_def(Just(Variant(True, "flag", $vartype))),
    $visitor_def(Just(Variant("somestr", "str", $vartype))),
    $visitor_def(Nothing(OptionalType($vartype))),
    $visitor_def(NULL)
;

$vartype1 = Variant<num1: Int32, num2: Int32, num3: Int32>;
SELECT
    VariantItem(Variant(7, "num2", $vartype1)),
    VariantItem(Just(Variant(5, "num1", $vartype1))),
    VariantItem(Nothing(OptionalType($vartype1))),
    VariantItem(NULL)
;