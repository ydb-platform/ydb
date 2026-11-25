#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <limits>

namespace NKikimr::NArrow::NAccessor {
namespace {

std::shared_ptr<arrow::Array> MakeValues(const std::vector<std::optional<i64>>& values) {
    arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(16));
    for (const auto value : values) {
        if (!value) {
            UNIT_ASSERT(builder.AppendNull().ok());
            continue;
        }

        std::array<ui8, 16> bytes;
        bytes.fill(*value < 0 ? 0xff : 0);
        for (ui32 i = 0; i < sizeof(i64); ++i) {
            bytes[i] = (static_cast<ui64>(*value) >> (8 * i)) & 0xff;
        }

        UNIT_ASSERT(builder.Append(bytes.data()).ok());
    }

    return builder.Finish().ValueOrDie();
}

void CheckBounds(const IChunkedArray& accessor, const NScheme::TTypeInfo& type, const std::shared_ptr<arrow::Scalar>& min,
    const std::shared_ptr<arrow::Scalar>& max) {
    const auto bounds = accessor.GetMinMaxScalars(type);
    UNIT_ASSERT(bounds.Min()->Equals(min));
    UNIT_ASSERT(bounds.Max()->Equals(max));
    UNIT_ASSERT(accessor.GetMaxScalar(type)->Equals(max));

    const auto restored = TMinMax::FromBinaryString(bounds.ToBinaryString(), accessor.GetDataType());
    UNIT_ASSERT(restored.CompareWithMin(min, type) == 0);
    UNIT_ASSERT(restored.CompareWithMax(max, type) == 0);
}

}   // namespace

Y_UNIT_TEST_SUITE(ArrowMinMaxLogicalTypes) {
    Y_UNIT_TEST(AccessorsUseLogicalTypeAndCurrentConfig) {
        TActorSystemStub actorSystem;
        const auto values = MakeValues({ std::nullopt, 1, 256, -1, -256, std::nullopt });
        const TTrivialArray plain(values);
        const TTrivialChunkedArray chunked(
            std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{ values->Slice(0, 3), values->Slice(3, 3) }));
        arrow::UInt32Builder positions;
        UNIT_ASSERT(positions.AppendValues({ 1, 2, 3, 4 }).ok());
        UNIT_ASSERT(positions.AppendNull().ok());
        const TDictionaryArray dictionary(values, positions.Finish().ValueOrDie());
        const auto sparse = TSparsedArray::Make(TTrivialArray(values->Slice(1, 4)), values->GetScalar(3).ValueOrDie());
        const auto sparseNullDefault = TSparsedArray::Make(plain, nullptr);
        const std::array<const IChunkedArray*, 5> accessors = { &plain, &chunked, &dictionary, sparse.get(), sparseNullDefault.get() };
        for (const bool numeric : { false, true }) {
            actorSystem.AppData.ColumnShardConfig.SetEnableDecimalNumericComparison(numeric);
            for (const auto& type : { NScheme::TTypeInfo(NScheme::TDecimalType(22, 0)), NScheme::TTypeInfo(NScheme::NTypeIds::Uuid) }) {
                const bool useNumeric = numeric && type.GetTypeId() == NScheme::NTypeIds::Decimal;
                const auto min = values->GetScalar(useNumeric ? 4 : 2).ValueOrDie();
                const auto max = values->GetScalar(useNumeric ? 2 : 3).ValueOrDie();
                for (const auto* accessor : accessors) {
                    CheckBounds(*accessor, type, min, max);
                }
            }
        }
    }

    Y_UNIT_TEST(SparseExtremaIgnoreUnusedDefault) {
        TActorSystemStub actorSystem;
        const NScheme::TTypeInfo type(NScheme::TDecimalType(22, 0));
        const auto values = MakeValues({ 1, 256 });
        const auto unusedDefault = MakeValues({ -1 })->GetScalar(0).ValueOrDie();
        const auto sparse = TSparsedArray::Make(TTrivialArray(values), unusedDefault);
        for (const bool numeric : { false, true }) {
            actorSystem.AppData.ColumnShardConfig.SetEnableDecimalNumericComparison(numeric);
            CheckBounds(*sparse, type, values->GetScalar(numeric ? 0 : 1).ValueOrDie(), values->GetScalar(numeric ? 1 : 0).ValueOrDie());
        }
    }

    Y_UNIT_TEST(EmptyAndNullChunksDoNotChangeExtrema) {
        TActorSystemStub actorSystem;
        const NScheme::TTypeInfo type(NScheme::TDecimalType(22, 0));
        const auto empty = MakeValues({});
        const auto nulls = MakeValues({ std::nullopt, std::nullopt });
        const auto values = MakeValues({ 1, 256 });
        const TTrivialChunkedArray chunked(std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{ nulls, empty, values, nulls }));
        for (const bool numeric : { false, true }) {
            actorSystem.AppData.ColumnShardConfig.SetEnableDecimalNumericComparison(numeric);
            for (const auto& array : { empty, nulls }) {
                const auto bounds = TMinMax::Compute(array, type);
                UNIT_ASSERT(bounds.IsNull());
                UNIT_ASSERT_VALUES_EQUAL(bounds.ToBinaryString(), "0");
                UNIT_ASSERT(TMinMax::FromBinaryString(bounds.ToBinaryString(), array->type()).IsNull());
            }

            CheckBounds(chunked, type, values->GetScalar(numeric ? 0 : 1).ValueOrDie(), values->GetScalar(numeric ? 1 : 0).ValueOrDie());
        }
    }

    Y_UNIT_TEST(PersistedOrderCannotBeReinterpretedAfterConfigChange) {
        TActorSystemStub actorSystem;
        const NScheme::TTypeInfo type(NScheme::TDecimalType(22, 0));
        const auto values = MakeValues({ 1, 2, 256 });
        const auto request = values->GetScalar(0).ValueOrDie();
        for (const bool numeric : { false, true }) {
            actorSystem.AppData.ColumnShardConfig.SetEnableDecimalNumericComparison(numeric);
            const auto data = TMinMax::Compute(values, type).ToBinaryString();
            UNIT_ASSERT_VALUES_EQUAL(data[0], numeric ? '2' : '1');
            const auto bounds = TMinMax::FromBinaryString(data, values->type());
            UNIT_ASSERT(bounds.CompareWithMin(request, type) != std::partial_ordering::unordered);
            UNIT_ASSERT(bounds.CompareWithMax(request, type) != std::partial_ordering::unordered);

            actorSystem.AppData.ColumnShardConfig.SetEnableDecimalNumericComparison(!numeric);
            UNIT_ASSERT(bounds.CompareWithMin(request, type) == std::partial_ordering::unordered);
            UNIT_ASSERT(bounds.CompareWithMax(request, type) == std::partial_ordering::unordered);
        }
    }

    Y_UNIT_TEST(FloatingPointNullAndNaNSemanticsArePreserved) {
        const NScheme::TTypeInfo type(NScheme::NTypeIds::Double);
        arrow::DoubleBuilder builder;
        UNIT_ASSERT(builder.AppendNull().ok());
        UNIT_ASSERT(builder.AppendValues({ std::numeric_limits<double>::quiet_NaN(), -2.0, 3.0 }).ok());
        const auto bounds = TMinMax::Compute(builder.Finish().ValueOrDie(), type);
        UNIT_ASSERT(bounds.Min()->Equals(arrow::DoubleScalar(-2.0)));
        UNIT_ASSERT(bounds.Max()->Equals(arrow::DoubleScalar(3.0)));
        const auto nan = std::make_shared<arrow::DoubleScalar>(std::numeric_limits<double>::quiet_NaN());
        UNIT_ASSERT(bounds.CompareWithMin(nan, type) == std::partial_ordering::unordered);
        UNIT_ASSERT(bounds.CompareWithMax(nan, type) == std::partial_ordering::unordered);
    }
}

}   // namespace NKikimr::NArrow::NAccessor
