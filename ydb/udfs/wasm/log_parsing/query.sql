/* syntax version 1 */
-- LogParsing smoke queries. Upload sdk + LogParsing WASM modules before running.

-- Two Protoseq frames: payloads "hello" and "world".
-- Layout per frame: ui32 LE length | payload | 32-byte framing syncword.
$protoseq_blob = String::HexDecode(
    "05000000"                 -- len=5
    || "68656c6c6f"           -- "hello"
    || "1ff7f77ebea65e9e37a6f62efeae47a7b76ebfaf169e9f37f657f766a706aff7"
    || "05000000"             -- len=5
    || "776f726c64"           -- "world"
    || "1ff7f77ebea65e9e37a6f62efeae47a7b76ebfaf169e9f37f657f766a706aff7"
);

SELECT
    LogParsing::LineBreak("a\nb\n\nc") AS line_break,
    LogParsing::LineBreak(NULL) AS line_break_null,
    LogParsing::Protoseq("") AS protoseq_empty,
    LogParsing::Protoseq($protoseq_blob) AS protoseq_blob,
    LogParsing::Protoseq(NULL) AS protoseq_null,
    LogParsing::ParseTskv("tskv\tdate=2024-01-01\tkey=value") AS parse_tskv,
    LogParsing::ParseTskv(NULL) AS parse_tskv_null,
    LogParsing::ParseTskv("not-tskv") AS parse_tskv_fail;
