$list = AsList(AsStruct('one' AS x), AsStruct('two' AS x));
$lazy = ListMap(ListFromRange(1s, 3s), ($i)->{ RETURN AsStruct($i AS y) });

SELECT *
    FROM AS_TABLE($list) as l
    CROSS JOIN AS_TABLE($lazy) as r;

SELECT *
    FROM AS_TABLE($lazy) as l
    CROSS JOIN AS_TABLE($list) as r;
