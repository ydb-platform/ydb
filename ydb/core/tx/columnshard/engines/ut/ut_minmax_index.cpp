#include <ydb/core/formats/arrow/accessor/abstract/minmax_utils.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/min_max/meta.h>

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

namespace {

using EOp = NArrow::NSSA::TIndexCheckOperation::EOperation;

TMinMax MakeChunk(i32 min, i32 max) {
    return *arrow::StructScalar::Make({ std::make_shared<arrow::Int32Scalar>(min), std::make_shared<arrow::Int32Scalar>(max) }, { "min", "max" })
                .ValueOrDie();
}

bool Skip(TMinMax indexValues, i32 request, EOp op) {
    return NOlap::NIndexes::NMinMax::TIndexMeta::Skip(
        indexValues, std::make_shared<arrow::Int32Scalar>(request), NArrow::NSSA::TIndexCheckOperation(op, true));
}

}   // namespace

Y_UNIT_TEST_SUITE(TMinMaxIndexSkipCorrectness) {
    Y_UNIT_TEST(Equals) {
        // skip: request outside [min, max]
        auto indexValues = MakeChunk(10, 20);
        UNIT_ASSERT(Skip(indexValues, 9, EOp::Equals));   // below min
        UNIT_ASSERT(Skip(indexValues, 21, EOp::Equals));   // above max
        // don't skip: request inside [min, max]
        UNIT_ASSERT(!Skip(indexValues, 10, EOp::Equals));   // at min
        UNIT_ASSERT(!Skip(indexValues, 15, EOp::Equals));   // inside
        UNIT_ASSERT(!Skip(indexValues, 20, EOp::Equals));   // at max
    }

    Y_UNIT_TEST(Less) {
        // WHERE col < request → skip iff request <= min
        auto indexValues = MakeChunk(10, 20);
        UNIT_ASSERT(Skip(indexValues, 9, EOp::Less));   // request < min
        UNIT_ASSERT(Skip(indexValues, 10, EOp::Less));   // request == min: nothing < min in indexValues
        UNIT_ASSERT(!Skip(indexValues, 11, EOp::Less));   // min < request, so min qualifies
        UNIT_ASSERT(!Skip(indexValues, 25, EOp::Less));   // request > max: whole indexValues qualifies
    }

    Y_UNIT_TEST(Greater) {
        // WHERE col > request → skip iff request >= max
        auto indexValues = MakeChunk(10, 20);
        UNIT_ASSERT(Skip(indexValues, 21, EOp::Greater));   // request > max
        UNIT_ASSERT(Skip(indexValues, 20, EOp::Greater));   // request == max: nothing > max in indexValues
        UNIT_ASSERT(!Skip(indexValues, 19, EOp::Greater));   // max > request, so max qualifies
        UNIT_ASSERT(!Skip(indexValues, 5, EOp::Greater));   // request < min: whole indexValues qualifies
    }

    Y_UNIT_TEST(LessOrEqual) {
        // WHERE col <= request → skip iff request < min
        auto indexValues = MakeChunk(10, 20);
        UNIT_ASSERT(Skip(indexValues, 9, EOp::LessOrEqual));   // request < min
        UNIT_ASSERT(!Skip(indexValues, 10, EOp::LessOrEqual));   // request == min: min <= min
        UNIT_ASSERT(!Skip(indexValues, 15, EOp::LessOrEqual));   // request inside range
        UNIT_ASSERT(!Skip(indexValues, 25, EOp::LessOrEqual));   // request > max: whole indexValues qualifies
    }

    Y_UNIT_TEST(GreaterOrEqual) {
        // WHERE col >= request → skip iff request > max
        auto indexValues = MakeChunk(10, 20);
        UNIT_ASSERT(Skip(indexValues, 21, EOp::GreaterOrEqual));   // request > max
        UNIT_ASSERT(!Skip(indexValues, 20, EOp::GreaterOrEqual));   // request == max: max >= max
        UNIT_ASSERT(!Skip(indexValues, 15, EOp::GreaterOrEqual));   // request inside range
        UNIT_ASSERT(!Skip(indexValues, 5, EOp::GreaterOrEqual));   // request < min: whole indexValues qualifies
    }

    Y_UNIT_TEST(NullChunk) {
        // all-NULL indexValues is always skipped for any non-null predicate
        auto indexValues = TMinMax::MakeNull(arrow::int32());
        UNIT_ASSERT(Skip(indexValues, 15, EOp::Equals));
        UNIT_ASSERT(Skip(indexValues, 15, EOp::Less));
        UNIT_ASSERT(Skip(indexValues, 15, EOp::Greater));
        UNIT_ASSERT(Skip(indexValues, 15, EOp::LessOrEqual));
        UNIT_ASSERT(Skip(indexValues, 15, EOp::GreaterOrEqual));
    }

}   // Y_UNIT_TEST_SUITE

}   // namespace NKikimr::NArrow::NAccessor
