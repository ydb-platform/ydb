SELECT
    Yson::ConvertTo(@@[{"year"="9999"; "a"="three"; "b"=3}; {"year"="9999"; "a"="four"; "b"=4}]@@y, List<Struct<>>) as list_of_empty_structs;
