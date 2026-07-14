/* syntax version 1 */
SELECT
    ProtoSimple::proto_roundtrip(42) AS value,
    ProtoSimple::proto_roundtrip(CAST(NULL AS Int64?)) AS null_value;
