/* syntax version 1 */
-- Basic Trie0001 fixture (wasm/trie/query.sql plus a string table).
-- Hit haystack is Ip::FromString("8000::") binary; miss is "::".
-- LookupWithString reads the payload as an absolute offset of an
-- (ui32 length, bytes) record, so the matching node points at the record
-- appended after the 16-byte header and the 32-byte trie.
-- Expected: hit=48, miss=-1, label='ok', nores=NULL
$dict = String::HexDecode(
    "5472696530303031"
    || "20000000"
    || "01000000"
    || "00000000"
    || "10000000"
    || "00000000"
    || "00000000"
    || "00000080"
    || "00000000"
    || "3000000000000000"
    || "02000000"
    || "6f6b"
);
$hit = String::HexDecode("80000000000000000000000000000000");
$miss = String::HexDecode("00000000000000000000000000000000");

SELECT
    TrieNative::Lookup($hit, $dict) AS hit,
    TrieNative::Lookup($miss, $dict) AS miss,
    TrieNative::LookupWithString($hit, $dict) AS label,
    TrieNative::LookupWithString($miss, $dict) AS nores;
