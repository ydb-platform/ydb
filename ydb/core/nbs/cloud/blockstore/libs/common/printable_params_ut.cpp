#include "printable_params.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>

namespace NYdb::NBS::NBlockStore {

namespace {

struct TSmallStruct
{
    ui32 A = 0;
    ui32 B = 0;
};

IOutputStream& operator<<(IOutputStream& out, TSmallStruct value)
{
    return out << value.A << "/" << value.B;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPrintableParams)
{
    Y_UNIT_TEST(PrintKeyValueTest)
    {
        const TPrintableParam params[] = {
            {"str", TString("hello")},
            {"cstr", static_cast<const char*>("world")},
            {"buf", TStringBuf("bufval")},
            {"i", int{-1}},
            {"u16", ui16{16}},
            {"u32", ui32{32}},
            {"u64", ui64{64}},
            {"range", TBlockRange64::MakeClosedInterval(0, 9)},
            {"flag", std::monostate{}},
        };
        UNIT_ASSERT_VALUES_EQUAL(
            "str:hello cstr:world buf:bufval i:-1 u16:16 u32:32 u64:64"
            " range:[0..9] flag",
            PrintParams(params));
    }

    Y_UNIT_TEST(PrintInlinePrintableTest)
    {
        const TPrintableParam params[] = {
            {"s", TSmallStruct{.A = 1, .B = 2}},
        };
        UNIT_ASSERT_VALUES_EQUAL("s:1/2", PrintParams(params));
    }
}

}   // namespace NYdb::NBS::NBlockStore
