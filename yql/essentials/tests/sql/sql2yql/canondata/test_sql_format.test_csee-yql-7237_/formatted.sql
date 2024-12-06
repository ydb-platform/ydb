/* syntax version 1 */
USE plato;
$push_final_data = AsList(
    AsStruct("manufacturer" AS manufacturer, "state" AS state)
);

INSERT INTO @push_final
SELECT
    *
FROM
    AS_TABLE($push_final_data)
;
COMMIT;
$manufacturer_name_fix = ($manufacturer) -> {
    $lowered_manufacturer = CAST(Unicode::ToLower(CAST(String::Strip($manufacturer) AS Utf8)) AS String);
    $in = AsList(
        "oysters", -- bullshit in naming
        "qumo", -- bullshit in naming
        "texet", -- bullshit in naming
        "alcatel", -- bullshit in naming
        "dexp", -- bullshit in naming
        "haier", -- bullshit in naming
        "dexp", -- bullshit in naming
        "asus", -- ASUSTek Computer Inc & ASUS both usable
        "yota", -- Yota Devices & Yota Devices Limited ...
        "ark" -- "ark" & "ark electronic technology" & "ark_electronic_technology"
    );
    $lambda = ($substring) -> {
        RETURN FIND($lowered_manufacturer, $substring) IS NULL;
    };
    $list = ListSkipWhile($in, $lambda);
    RETURN IF(ListHasItems($list), $list[0], $lowered_manufacturer);
};

$manufacturers_whitelist = (
    SELECT
        man AS manufacturer
    FROM (
        SELECT
            man,
            COUNT(*) AS cnt
        FROM
            @push_final
        GROUP BY
            $manufacturer_name_fix(manufacturer) AS man
    )
    WHERE
        cnt > 1000
);

$push_final_preprocessing = (
    SELECT
        $manufacturer_name_fix(manufacturer) AS manufacturer,
        state
    FROM
        @push_final
);

SELECT
    COALESCE(fixed_manufacturer, "other") AS manufacturer,
    L.*
WITHOUT
    L.manufacturer
FROM
    $push_final_preprocessing AS L
LEFT JOIN (
    SELECT
        manufacturer AS fixed_manufacturer
    FROM
        $manufacturers_whitelist
) AS R
ON
    (L.manufacturer == R.fixed_manufacturer)
;
