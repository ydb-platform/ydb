SELECT
    y,
    FormatType(TypeOf(y))
FROM
    AS_TABLE([<|x: Yson('[1;2;3]')|>])
    FLATTEN LIST BY (
        Yson::ConvertToList(x) AS y
    )
;
