/* postgres can not */
USE plato;

$improve_low = ($val) -> {
    RETURN CAST($val AS Utf8);
};

$names_intersection = ($org_names, $db_names) -> {
    RETURN ListLength(
        ListFlatten(
            ListMap(
                $org_names,
                ($org_name) -> {
                    RETURN ListFilter(
                        $db_names,
                        ($db_name) -> {
                            $org_name = $improve_low($org_name);
                            $db_name = $improve_low($db_name);
                            RETURN Unicode::LevensteinDistance($org_name, $db_name) < 0.2 * Unicode::GetLength($org_name);
                        }
                    );
                }
            )
        )
    ) > 0;
};

select $names_intersection(['1', '2'], ['nets'])
