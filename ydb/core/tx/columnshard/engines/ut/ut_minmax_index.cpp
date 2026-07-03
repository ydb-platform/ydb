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
    // indexValues = [10, 20]; request values: 9 (below min), 10 (at min), 15 (inside), 20 (at max), 21 (above max)

    Y_UNIT_TEST(Equals) {
        // skip iff request < min OR request > max
        auto indexValues = MakeChunk(10, 20);
        UNIT_ASSERT(Skip(indexValues, 9, EOp::Equals));
        UNIT_ASSERT(!Skip(indexValues, 10, EOp::Equals));
        UNIT_ASSERT(!Skip(indexValues, 15, EOp::Equals));
        UNIT_ASSERT(!Skip(indexValues, 20, EOp::Equals));
        UNIT_ASSERT(Skip(indexValues, 21, EOp::Equals));
    }

    Y_UNIT_TEST(Less) {
        // WHERE col < request → skip iff request <= min
        auto indexValues = MakeChunk(10, 20);
        UNIT_ASSERT(Skip(indexValues, 9, EOp::Less));
        UNIT_ASSERT(Skip(indexValues, 10, EOp::Less));
        UNIT_ASSERT(!Skip(indexValues, 15, EOp::Less));
        UNIT_ASSERT(!Skip(indexValues, 20, EOp::Less));
        UNIT_ASSERT(!Skip(indexValues, 21, EOp::Less));
    }

    Y_UNIT_TEST(Greater) {
        // WHERE col > request → skip iff request >= max
        auto indexValues = MakeChunk(10, 20);
        UNIT_ASSERT(!Skip(indexValues, 9, EOp::Greater));
        UNIT_ASSERT(!Skip(indexValues, 10, EOp::Greater));
        UNIT_ASSERT(!Skip(indexValues, 15, EOp::Greater));
        UNIT_ASSERT(Skip(indexValues, 20, EOp::Greater));
        UNIT_ASSERT(Skip(indexValues, 21, EOp::Greater));
    }

    Y_UNIT_TEST(LessOrEqual) {
        // WHERE col <= request → skip iff request < min
        auto indexValues = MakeChunk(10, 20);
        UNIT_ASSERT(Skip(indexValues, 9, EOp::LessOrEqual));
        UNIT_ASSERT(!Skip(indexValues, 10, EOp::LessOrEqual));
        UNIT_ASSERT(!Skip(indexValues, 15, EOp::LessOrEqual));
        UNIT_ASSERT(!Skip(indexValues, 20, EOp::LessOrEqual));
        UNIT_ASSERT(!Skip(indexValues, 21, EOp::LessOrEqual));
    }

    Y_UNIT_TEST(GreaterOrEqual) {
        // WHERE col >= request → skip iff request > max
        auto indexValues = MakeChunk(10, 20);
        UNIT_ASSERT(!Skip(indexValues, 9, EOp::GreaterOrEqual));
        UNIT_ASSERT(!Skip(indexValues, 10, EOp::GreaterOrEqual));
        UNIT_ASSERT(!Skip(indexValues, 15, EOp::GreaterOrEqual));
        UNIT_ASSERT(!Skip(indexValues, 20, EOp::GreaterOrEqual));
        UNIT_ASSERT(Skip(indexValues, 21, EOp::GreaterOrEqual));
    }

    Y_UNIT_TEST(NullChunk) {
        // all-NULL chunk is always skipped for any non-null predicate
        auto indexValues = TMinMax::MakeNull(arrow::int32());
        UNIT_ASSERT(Skip(indexValues, 15, EOp::Equals));
        UNIT_ASSERT(Skip(indexValues, 15, EOp::Less));
        UNIT_ASSERT(Skip(indexValues, 15, EOp::Greater));
        UNIT_ASSERT(Skip(indexValues, 15, EOp::LessOrEqual));
        UNIT_ASSERT(Skip(indexValues, 15, EOp::GreaterOrEqual));
    }

}   // Y_UNIT_TEST_SUITE

}   // namespace NKikimr::NArrow::NAccessor
