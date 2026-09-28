#include <ydb/udfs/wasm/trie/binary_trie.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/string/hex.h>

using namespace NBinaryTrie;

namespace {

TString BasicDictV1() {
    // Same blob as kikimr/yq/udfs/trie Basic.sql $dictv1
    return HexDecode(
        "5472696530303031"
        "20000000"
        "01000000"
        "00000000"
        "10000000"
        "00000000"
        "00000000"
        "00000080"
        "00000000"
        "0a00000000000000");
}

TString IpV6(ui8 firstByte) {
    TString out(16, '\0');
    out[0] = static_cast<char>(firstByte);
    return out;
}

} // namespace

Y_UNIT_TEST_SUITE(TBinaryTrieTest) {

Y_UNIT_TEST(LookupBasicV1) {
    const TString dict = BasicDictV1();
    UNIT_ASSERT_VALUES_EQUAL(LookupTrie(IpV6(0x80), dict), 10);
    UNIT_ASSERT_VALUES_EQUAL(LookupTrie(IpV6(0x00), dict), -1);
}

Y_UNIT_TEST(LookupRejectsBadSignature) {
    const TString bad = "NotATrie______";
    UNIT_ASSERT_EXCEPTION(LookupTrie(IpV6(0x80), bad), yexception);
}

} // Y_UNIT_TEST_SUITE
