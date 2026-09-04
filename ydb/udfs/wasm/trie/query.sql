/* syntax version 1 */
-- Basic Trie0001 fixture (same as kikimr/yq/udfs/trie Basic.sql $dictv1).
-- Hit haystack is Ip::FromString("8000::") binary; miss is "::".
-- Expected: hit=10, miss=-1, nores=NULL
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
    Trie::Lookup($hit, $dict) AS hit,
    Trie::Lookup($miss, $dict) AS miss,
    Trie::Lookup(NULL, $dict) AS nores;

-- Bonus LookupCached (fixed dict via TypeConfig):
-- $fn = YQL::Udf(AsAtom("Trie.LookupCached"), Void(), Void(), AsAtom(<dict-as-atom>));
-- SELECT $fn($hit);
