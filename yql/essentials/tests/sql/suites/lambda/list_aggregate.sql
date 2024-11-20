/* syntax version 1 */
$subst = ($names, $indices) -> {
    RETURN ListMap(
        $indices,
        ($index) -> {
            RETURN $names[$index];
        }
    );
};

$table = (
    SELECT AsList("a", "b") AS names, AsList(0, 0, 1) AS indices
    UNION ALL
    SELECT AsList("c", "d") AS names, AsList(0, 1, 1) AS indices
);

SELECT AGGREGATE_LIST($subst(names, indices))
FROM $table;
