#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(Self) {

    Y_UNIT_TEST(Literals)
    {
        using namespace NTest;
        using TPut = NTest::TCookRow;

        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::String)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Col(0, 2,  NScheme::NTypeIds::Double)
            .Col(0, 3,  NScheme::NTypeIds::Bool)
            .Key({0, 1});

        auto put = *TPut().Do(0, "foo").Do<double>(2, 3.14).Do<ui32>(1, 42);
        auto natTail = *TSchemedCookRow(*lay).Col("foo", 42_u32, 3.14);
        auto natNull = *TSchemedCookRow(*lay).Col("foo", 42_u32, 3.14, nullptr);

        /*_ Different binary rows representations should have the same
            textual output. Along with check that test rows wrappers
            really cooks materialized rows with expected result.
         */

        auto *lit = "(String : foo, Uint32 : 42, Double : 3.14, Bool : NULL)";

        const auto check = [=](const TString &sample)
        {
            if (sample != lit) {
                Cerr
                    << "Row validation failed: "
                    << NUnitTest::ColoredDiff(lit, sample)
                    << Endl;

                UNIT_ASSERT(false);
            }
        };

        check(TRowTool(*lay).Describe(put));
        check(TRowTool(*lay).Describe(natTail));
        check(TRowTool(*lay).Describe(natNull));
    }

};

}
}
