#include "mkql_alloc.h"
#include "mkql_unboxed_value_stream.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;
using namespace NKikimr::NMiniKQL;

Y_UNIT_TEST_SUITE(TMiniKQLUnboxedValueStream) {
    Y_UNIT_TEST(Output) {
        TScopedAlloc alloc(__LOCATION__);
        TUnboxedValueStream out;
        for (int i = -128; i <= 127; ++i) {
            out << char(i);
        }
        const NUdf::TUnboxedValue val = out.Value();
        UNIT_ASSERT(val.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(256, val.AsStringRef().Size());

        const NUdf::TUnboxedValue remains = out.Value();
        UNIT_ASSERT(!remains.HasValue());
    }
}
