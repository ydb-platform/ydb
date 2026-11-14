PRAGMA Library("aggregate.sql");
IMPORT aggregate SYMBOLS $DigestFactory;

SELECT AGGREGATE_BY(TableRow(), $DigestFactory("Unknown"))
FROM AS_TABLE(AsList(AsStruct(1ul AS x)))
WHERE x < 0ul;
