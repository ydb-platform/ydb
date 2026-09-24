/* syntax version 1 */
-- LogParsing smoke queries. Upload sdk + LogParsing WASM modules before running.

-- The caller supplies the framing syncword.
$syncword = String::HexDecode("1ff7f77ebea65e9e37a6f62efeae47a7b76ebfaf169e9f37f657f766a706aff7");

-- Two Protoseq frames: payloads "hello" and "world".
-- Layout per frame: ui32 LE length | payload | syncword.
$protoseq_blob = String::HexDecode(
    "05000000"                 -- len=5
    || "68656c6c6f"           -- "hello"
) || $syncword || String::HexDecode(
    "05000000"                 -- len=5
    || "776f726c64"           -- "world"
) || $syncword;

SELECT
    LogParsing::LineBreak("a\nb\n\nc") AS line_break,
    LogParsing::LineBreak(NULL) AS line_break_null,
    LogParsing::Protoseq("", $syncword) AS protoseq_empty,
    LogParsing::Protoseq($protoseq_blob, $syncword) AS protoseq_blob,
    LogParsing::Protoseq(String::HexDecode("01000000615a5a"), "ZZ") AS protoseq_custom_syncword,
    LogParsing::Protoseq(NULL, $syncword) AS protoseq_null,
    LogParsing::Protoseq($protoseq_blob, NULL) AS protoseq_no_syncword,
    LogParsing::ParseTskv("tskv\tdate=2024-01-01\tkey=value") AS parse_tskv,
    LogParsing::ParseTskv(NULL) AS parse_tskv_null,
    LogParsing::ParseTskv("not-tskv") AS parse_tskv_fail;
