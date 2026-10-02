/* custom error: Failed to evaluate unresolved argument: $name. Did you use a column? */
$names = [['a', 'b'], ['c', 'd']];
$s = <|a: 1, b: 2, c: 3, d: 4|>;

SELECT
    ListMap(
        $names, ($name) -> {
            RETURN ChooseMembers($s, $name);
        }
    )
;
