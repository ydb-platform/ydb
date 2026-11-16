PRAGMA Library("aggregate.sql");
IMPORT aggregate SYMBOLS $DigestFactory;

$rows = AsList(AsStruct(1ul AS x), AsStruct(2ul AS x), AsStruct(3ul AS x));

$check = ($row) -> {
    RETURN AsStruct(
        $row.g AS g, $row.n AS n,
        $row.city64.count = $row.n AS count64_matches,
        $row.city128.count = $row.n AS count128_matches,
        $row.sha256.count = $row.n AS count256_matches,
        $row.city64.hash AS hash64, $row.city128.hash AS hash128, $row.sha256.hash AS hash256
    );
};

-- Groups of sizes one and two must keep separate mutable states.
$varying = (
    SELECT
        g, COUNT(*) AS n,
        AGGREGATE_BY(AsStruct(x AS x), $DigestFactory("CityHash64")) AS city64,
        AGGREGATE_BY(AsStruct(x AS x), $DigestFactory("CityHash128")) AS city128,
        AGGREGATE_BY(AsStruct(x AS x), $DigestFactory("Sha256")) AS sha256
    FROM AS_TABLE($rows)
    GROUP BY x > 1ul AS g
);
SELECT $check(TableRow()) AS result FROM $varying ORDER BY g;

-- Even identical serialized input values must not share state across groups.
$constant = (
    SELECT
        g, COUNT(*) AS n,
        AGGREGATE_BY(AsStruct(1ul AS x), $DigestFactory("CityHash64")) AS city64,
        AGGREGATE_BY(AsStruct(1ul AS x), $DigestFactory("CityHash128")) AS city128,
        AGGREGATE_BY(AsStruct(1ul AS x), $DigestFactory("Sha256")) AS sha256
    FROM AS_TABLE($rows)
    GROUP BY x > 1ul AS g
);
SELECT $check(TableRow()) AS result FROM $constant ORDER BY g;
