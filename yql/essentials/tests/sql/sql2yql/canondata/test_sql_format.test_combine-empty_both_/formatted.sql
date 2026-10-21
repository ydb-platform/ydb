$f = ($key, $leftList, $rightList) -> {
    RETURN <|k: $key, v: ListZipAll($leftList, $rightList)|>;
};

$a = ListCreate(Struct<key: Int32, subkey: Int32, value: String>);

$b = ListCreate(Struct<key: Int32, subkey: Int32, value: String>);

COMBINE AS_TABLE($a) AS A
    PRESORT
        A.key
WITH AS_TABLE($b) AS B
    PRESORT
        B.key
ON
    A.key == B.key
USING $f(TableRow(), TableRow());
