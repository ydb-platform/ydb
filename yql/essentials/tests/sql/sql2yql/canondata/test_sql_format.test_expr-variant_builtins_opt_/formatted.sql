$vartype = Variant<a: Optional<String>, b: Optional<String>>;

$handle_a = ($x) -> {
    RETURN CAST(($x || "1") AS Uint32);
};

$handle_b = ($x) -> {
    RETURN CAST(($x || "2") AS Uint32);
};

$var_a = VARIANT ("5", "a", $vartype);
$var_b = VARIANT ("6", "b", $vartype);

SELECT
    Visit(Just($var_a), $handle_a AS a, $handle_b AS b),
    Visit(Just($var_b), $handle_a AS a, $handle_b AS b),
    VisitOrDefault(Just($var_b), Just(777u), $handle_a AS a),
    VariantItem(Just($var_b))
;

$vartype_t = Variant<Optional<String>, Optional<String>>;
$var_1 = VARIANT ("7", "0", $vartype_t);
$var_2 = VARIANT ("8", "1", $vartype_t);

SELECT
    Visit(Just($var_1), $handle_a, $handle_b),
    Visit(Just($var_2), $handle_a, $handle_b),
    VisitOrDefault(Just($var_2), $handle_a, Just(777u)),
    VariantItem(Just($var_b))
;
