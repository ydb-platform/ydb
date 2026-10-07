$f = ($key, $leftList, $rightList) -> {
    RETURN <|k: $key, v: ListZipAll($leftList, $rightList)|>;
};

$a = [
    <|key: 1ul, subkey: 11, value: '111'|>,
    <|key: 1ul, subkey: 12, value: '112'|>,
    <|key: 2ul, subkey: 21, value: '221'|>,
    <|key: 2ul, subkey: 22, value: '222'|>,
    <|key: 3ul, subkey: 33, value: '333'|>,
];

$b = [
    <|key: Just(1), subkey: 11, value: '111'|>,
    <|key: Just(2), subkey: 22, value: '222'|>,
    <|key: NULL, subkey: 33, value: '333'|>,
    <|key: Just(4), subkey: 44, value: '444'|>,
    <|key: Just(5), subkey: 55, value: '555'|>,
];

COMBINE AS_TABLE($a) AS A
    PRESORT
        A.key,
        A.subkey
WITH AS_TABLE($b) AS B
    PRESORT
        B.key,
        B.subkey
ON
    A.key == B.key AND A.subkey == B.subkey
USING $f(TableRow(), TableRow());
