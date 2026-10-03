$p = RandomNumber(1);

SELECT Uuid::newV8ColumnKey() != Uuid::newV8ColumnKey() AS column_key_unique;
SELECT Uuid::newV8RowKey() != Uuid::newV8RowKey() AS row_key_unique;

SELECT Uuid::newV8ColumnKey(1) != Uuid::newV8ColumnKey(2) AS column_key_dep_unique;
SELECT Uuid::newV8RowKey(1) != Uuid::newV8RowKey(2) AS row_key_dep_unique;
SELECT Uuid::newV8ColumnKey(1, 2, 3) != Uuid::newV8ColumnKey(1, 2, 4) AS column_key_three_dep_unique;
SELECT Uuid::newV8RowKey(1, 2, 3) != Uuid::newV8RowKey(1, 2, 4) AS row_key_three_dep_unique;

$group = Uuid::newV8RowGroup($p, 3ul);
SELECT ListLength($group) = 3ul AS row_group_count;
SELECT Unwrap($group[0]) != Unwrap($group[1]) AND Unwrap($group[1]) != Unwrap($group[2]) AS row_group_distinct;

$groupFromUuid = Uuid::newV8RowGroup(Unwrap($group[0]), 2ul);
SELECT ListLength($groupFromUuid) = 2ul AS row_group_uuid_prefix_count;
SELECT Unwrap($groupFromUuid[0]) != Unwrap($groupFromUuid[1]) AS row_group_uuid_prefix_distinct;

$groupDep = Uuid::newV8RowGroup($p, 2ul, 1);
$groupDep2 = Uuid::newV8RowGroup($p, 2ul, 2);
SELECT Unwrap($groupDep[0]) != Unwrap($groupDep2[0]) AS row_group_dep_unique;

SELECT
    Substring(CAST(Uuid::newV8ColumnKey() AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newV8ColumnKey() AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newV8ColumnKey() AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newV8ColumnKey() AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newV8ColumnKey() AS String), 14, 1) = '8'
    AS column_key_string_format;
SELECT
    Substring(CAST(Uuid::newV8RowKey() AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newV8RowKey() AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newV8RowKey() AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newV8RowKey() AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newV8RowKey() AS String), 14, 1) = '8'
    AS row_key_string_format;
SELECT
    Substring(CAST(Unwrap($group[0]) AS String), 14, 1) = '8'
    AS row_group_string_format;

SELECT Uuid::newV4() != Uuid::newV4() AS v4_unique;
SELECT Uuid::newV4(1) != Uuid::newV4(2) AS v4_dep_unique;
SELECT
    Substring(CAST(Uuid::newV4() AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newV4() AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newV4() AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newV4() AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newV4() AS String), 14, 1) = '4'
    AS v4_string_format;

SELECT Uuid::newV7() != Uuid::newV7() AS v7_unique;
SELECT Uuid::newV7(1) != Uuid::newV7(2) AS v7_dep_unique;
SELECT
    Substring(CAST(Uuid::newV7() AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newV7() AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newV7() AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newV7() AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newV7() AS String), 14, 1) = '7'
    AS v7_string_format;

$ts = CurrentUtcTimestamp();
$ts64 = CAST($ts AS Timestamp64);
SELECT Uuid::newV7At($ts) != Uuid::newV7At($ts) AS v7_at_unique;
SELECT Uuid::newV7At($ts64) != Uuid::newV7At($ts64) AS v7_at_ts64_unique;
SELECT Uuid::newV7At($ts, 1) != Uuid::newV7At($ts, 2) AS v7_at_dep_unique;
SELECT
    Substring(CAST(Uuid::newV7At($ts) AS String), 14, 1) = '7'
    AS v7_at_string_format;

$fixed_ts = CAST(1700000000123000ul AS Timestamp);
$fixed_ts64 = CAST(1700000000123000l AS Timestamp64);
SELECT Uuid::extractTs(Uuid::newV7At($fixed_ts)) = $fixed_ts AS v7_extract_ts_roundtrip;
SELECT Uuid::extractTs64(Uuid::newV7At($fixed_ts64)) = $fixed_ts64 AS v7_extract_ts64_roundtrip;
SELECT Uuid::extractTs(Uuid::newV8ColumnKey()) IS NULL AS v7_extract_ts_column_key_null;
SELECT Uuid::extractTs(Uuid::newV8RowKey()) IS NULL AS v7_extract_ts_row_key_null;
SELECT Uuid::extractTs(Uuid::newV4()) IS NULL AS v7_extract_ts_v4_null;

SELECT $p != 0ul OR $p == 0ul AS prefix_is_uint64;
