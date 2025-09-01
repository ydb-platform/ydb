USE plato;

$tableList = AsList(
    AsStruct(Yson('{"row_count"=9}') as Attributes, "Input1" as Path, "table" as Type),
    AsStruct(Yson('{"row_count"=19}') as Attributes, "Input2" as Path, "table" as Type)
);

-- $bucket_size = 1000000;
$buckets = ASLIST(0, 1, 2, 3);

$row_count = (
    SELECT Yson::LookupInt64(Attributes, "row_count")
    FROM AS_TABLE($tableList)
    WHERE
        Type = "table"
);

$bucket_size = unwrap(CAST($row_count / ListLength($buckets) AS Uint64));

DEFINE ACTION $make_bucket($bucket_number) AS
    $offset = unwrap(CAST($bucket_number AS UInt8)) * $bucket_size;
    $dst = "Output" || $bucket_number;

    INSERT INTO $dst (
    SELECT * FROM Input
    ORDER BY key
    LIMIT $bucket_size OFFSET $offset);
END DEFINE;

EVALUATE FOR $bucket_number IN $buckets
    DO $make_bucket(CAST($bucket_number AS String));
