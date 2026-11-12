/* syntax version 1 */
-- LogParsing smoke queries. Upload sdk + LogParsing WASM modules before running.

-- Use an arbitrary syncword to show that the caller supplies it.
$syncword = "ZZ";

-- Two Protoseq frames: payloads "hello" and "world".
-- Layout per frame: ui32 LE length | payload | syncword.
$protoseq_blob = String::HexDecode(
    "05000000"                 -- len=5
    || "68656c6c6f"           -- "hello"
) || $syncword || String::HexDecode(
    "05000000"                 -- len=5
    || "776f726c64"           -- "world"
) || $syncword;

-- A corrupt middle frame must fail the whole chunk, even if a later frame is valid.
$corrupt_middle = String::HexDecode("030000006f6e65") || $syncword
    || String::HexDecode("6400000074776f") || $syncword
    || String::HexDecode("050000007468726565") || $syncword;
$leading_junk = "junk" || $syncword || $protoseq_blob;

SELECT
    LogParsing::LineBreak("a\nb\n\nc") AS line_break,
    LogParsing::LineBreak(NULL) AS line_break_null,
    LogParsing::Protoseq("", $syncword) AS protoseq_empty,
    LogParsing::Protoseq($protoseq_blob, $syncword) AS protoseq_blob,
    LogParsing::Protoseq(String::HexDecode("01000000615959"), "YY") AS protoseq_custom_syncword,
    LogParsing::Protoseq($corrupt_middle, $syncword) AS protoseq_corrupt_middle,
    LogParsing::Protoseq($leading_junk, $syncword) AS protoseq_leading_junk,
    LogParsing::Protoseq(NULL, $syncword) AS protoseq_null,
    LogParsing::Protoseq($protoseq_blob, NULL) AS protoseq_no_syncword,
    LogParsing::ParseTskv("tskv\tdate=2024-01-01\tkey=value") AS parse_tskv,
    LogParsing::ParseTskv(NULL) AS parse_tskv_null,
    LogParsing::ParseTskv("not-tskv") AS parse_tskv_fail;
