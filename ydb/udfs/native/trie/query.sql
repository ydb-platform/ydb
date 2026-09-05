/* syntax version 1 */
-- Basic Trie0001 fixture (same as wasm/trie/query.sql).
-- Hit haystack is Ip::FromString("8000::") binary; miss is "::".
-- Expected: hit=10, miss=-1, label nonempty on hit, nores=NULL
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
    || "0a00000000000000"
);
$hit = String::HexDecode("80000000000000000000000000000000");
$miss = String::HexDecode("00000000000000000000000000000000");

SELECT
    TrieNative::Lookup($hit, $dict) AS hit,
    TrieNative::Lookup($miss, $dict) AS miss,
    TrieNative::LookupWithString($hit, $dict) AS label,
    TrieNative::LookupWithString($miss, $dict) AS nores;
