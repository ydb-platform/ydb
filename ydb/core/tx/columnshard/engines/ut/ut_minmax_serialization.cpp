#include <ydb/core/formats/arrow/accessor/abstract/minmax_utils.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NArrow::NAccessor {

namespace {

void CheckRoundTrip(const TMinMax& original) {
    TString serialized = original.ToBinaryString();
    TMinMax restored = TMinMax::FromBinaryString(serialized, original.Min()->type);

    if (original.IsNull()) {
        UNIT_ASSERT(restored.IsNull());
    } else {
        UNIT_ASSERT(!restored.IsNull());
        UNIT_ASSERT(original.Min()->Equals(restored.Min()));
        UNIT_ASSERT(original.Max()->Equals(restored.Max()));
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(TMinMaxSerializationTests) {

    Y_UNIT_TEST(NullRoundTrip) {
        auto type = arrow::int32();
        TMinMax minmax = TMinMax::MakeNull(type);
        CheckRoundTrip(minmax);
    }

    Y_UNIT_TEST(Int32RoundTrip) {
        auto type = arrow::int32();
        auto minScalar = std::make_shared<arrow::Int32Scalar>(-100);
        auto maxScalar = std::make_shared<arrow::Int32Scalar>(200);
        TMinMax minmax = *arrow::StructScalar::Make({ minScalar, maxScalar }, { "min", "max" }).ValueOrDie();
        CheckRoundTrip(minmax);
    }

}

}   // namespace NKikimr::NArrow::NAccessor
