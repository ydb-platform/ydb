#include <library/cpp/scheme/scheme.h>
#include <library/cpp/scheme/tests/fuzz_ops/lib/fuzz_ops.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/hex.h>

Y_UNIT_TEST_SUITE(TTestSchemeFuzzOpsFoundBugs) {
    using namespace NSc::NUt;

    Y_UNIT_TEST(TestBug1) {
        FuzzOps(HexDecode("98040129000525"), true);
    }
}
