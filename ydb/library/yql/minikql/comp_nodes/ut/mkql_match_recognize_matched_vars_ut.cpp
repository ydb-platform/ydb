#include "../mkql_match_recognize_matched_vars.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

Y_UNIT_TEST_SUITE(MatchRecognizeMatchedVars) {
        TMemoryUsageInfo memUsage("MatchedVars");
        Y_UNIT_TEST(MatchedVarsEmpty) {
            TScopedAlloc alloc(__LOCATION__);
            {
                TMatchedVars vars{};
                NUdf::TUnboxedValue value(NUdf::TUnboxedValuePod(new TMatchedVarsValue(&memUsage, vars)));
                UNIT_ASSERT(value.HasValue());
            }
        }
        Y_UNIT_TEST(MatchedVars) {
            TScopedAlloc alloc(__LOCATION__);
            {
                TMatchedVar A{{1, 4}, {7, 9}, {100, 200}};
                TMatchedVar B{{1, 6}};
                TMatchedVars vars{A, B};
                NUdf::TUnboxedValue value(NUdf::TUnboxedValuePod(new TMatchedVarsValue(&memUsage, vars)));
                UNIT_ASSERT(value.HasValue());
                auto a = value.GetElement(0);
                UNIT_ASSERT(a.HasValue());
                UNIT_ASSERT_VALUES_EQUAL(3, a.GetListLength());
                auto iter = a.GetListIterator();
                UNIT_ASSERT(iter.HasValue());
                NUdf::TUnboxedValue last;
                while (iter.Next(last))
                    ;
                UNIT_ASSERT(last.HasValue());
                UNIT_ASSERT_VALUES_EQUAL(100, last.GetElement(0).Get<ui64>());
                UNIT_ASSERT_VALUES_EQUAL(200, last.GetElement(1).Get<ui64>());
                auto b = value.GetElement(1);
                UNIT_ASSERT(b.HasValue());
                UNIT_ASSERT_VALUES_EQUAL(1, b.GetListLength());
            }
        }
}
}//namespace NKikimr::NMiniKQL::TMatchRecognize
