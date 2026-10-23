#include <ydb/library/formats/arrow/replace_key.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <cstring>
#include <string>
#include <type_traits>

namespace NKikimr::NArrow {
namespace {

using TLogicalTypes = std::vector<NScheme::TTypeInfo>;
using TArrayCompare = std::partial_ordering (TComparator::*)(
    const arrow::Array&, int, const arrow::Array&, int, const NScheme::TTypeInfo&) const;
using TValueCompare = std::partial_ordering (TComparator::*)(const ui64&, const ui64&, const NScheme::TTypeInfo&) const;

constexpr TComparator LegacyComparator(EDecimalComparisonMode::Legacy);
constexpr TComparator NumericComparator(EDecimalComparisonMode::Numeric);

static_assert(!std::is_default_constructible_v<TComparator>);

static_assert(std::is_same_v<decltype(&TComparator::TypedCompare<false>), TArrayCompare>);
static_assert(std::is_same_v<decltype(&TComparator::CompareValueNotNull<ui64>), TValueCompare>);
static_assert(!std::is_constructible_v<TReplaceKey, std::shared_ptr<TArrayVec>, ui64>);
static_assert(!std::is_constructible_v<TReplaceKeyView, const TArrayVec&, ui64>);
static_assert(!std::is_constructible_v<TReplaceKey, std::shared_ptr<TArrayVec>, ui64, const TLogicalTypes&>);
static_assert(!std::is_constructible_v<TReplaceKeyView, const TArrayVec&, ui64, const TLogicalTypes&>);
static_assert(std::is_constructible_v<TReplaceKey, std::shared_ptr<TArrayVec>, ui64, TLogicalTypes&&, const TComparator&>);
static_assert(std::is_constructible_v<TReplaceKey, std::shared_ptr<TArrayVec>, ui64, const TLogicalTypes&&, const TComparator&>);
static_assert(std::is_constructible_v<TRawReplaceKey, const TArrayVec*, ui64, const TLogicalTypes&, const TComparator&>);
static_assert(std::is_constructible_v<TReplaceKeyView, const TArrayVec&, ui64, const TLogicalTypes&, const TComparator&>);
static_assert(!std::is_constructible_v<TRawReplaceKey, const TArrayVec*, ui64, TLogicalTypes&&, const TComparator&>);
static_assert(!std::is_constructible_v<TRawReplaceKey, const TArrayVec*, ui64, const TLogicalTypes&&, const TComparator&>);
static_assert(!std::is_constructible_v<TReplaceKeyView, const TArrayVec&, ui64, TLogicalTypes&&, const TComparator&>);
static_assert(!std::is_constructible_v<TReplaceKeyView, const TArrayVec&, ui64, const TLogicalTypes&&, const TComparator&>);

template <class T>
concept CArrayCompareWithoutLogicalType =
    requires(const T& value, const TComparator& comparator) { comparator.TypedCompare<false>(value, 0, value, 0); };

template <class T>
concept CValueCompareWithoutLogicalType =
    requires(const T& value, const TComparator& comparator) { comparator.CompareValueNotNull(value, value); };

template <class T>
concept CScalarCompareWithoutLogicalType = requires(const T& value) { ScalarCompare(value, value); };

static_assert(!CScalarCompareWithoutLogicalType<arrow::Scalar>);
static_assert(!CScalarCompareWithoutLogicalType<std::shared_ptr<arrow::Scalar>>);
static_assert(!CArrayCompareWithoutLogicalType<arrow::Array>);
static_assert(!CValueCompareWithoutLogicalType<ui64>);

template <class T>
concept CScalarCompareWithoutComparator = requires(const T& value, const NScheme::TTypeInfo& type) { ScalarCompare(value, value, type); };

static_assert(!CScalarCompareWithoutComparator<arrow::Scalar>);
static_assert(!CScalarCompareWithoutComparator<std::shared_ptr<arrow::Scalar>>);

TLogicalTypes MakeLogicalTypes() {
    return { NScheme::TTypeInfo(NScheme::TDecimalType(22, 9)), NScheme::TTypeInfo(NScheme::TDecimalType(35, 2)),
        NScheme::TTypeInfo(NScheme::NTypeIds::Uuid) };
}

std::shared_ptr<arrow::RecordBatch> MakeBatch() {
    const auto type = arrow::fixed_size_binary(16);
    TArrayVec columns;
    for (char column = 0; column < 3; ++column) {
        arrow::FixedSizeBinaryBuilder builder(type);
        for (const char firstByte : { 0, 0, 1 }) {
            std::string value(16, '\0');
            value[0] = firstByte;
            value[1] = column;
            UNIT_ASSERT(builder.Append(value).ok());
        }

        std::shared_ptr<arrow::Array> values;
        UNIT_ASSERT(builder.Finish(&values).ok());
        columns.emplace_back(std::move(values));
    }
    const auto schema = arrow::schema({ arrow::field("decimal22", type), arrow::field("decimal35", type), arrow::field("uuid", type) });
    return arrow::RecordBatch::Make(schema, 3, std::move(columns));
}

void CheckLogicalTypes(const TLogicalTypes& actual, const TLogicalTypes& expected) {
    UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        UNIT_ASSERT_C(actual[i] == expected[i], "Logical type differs at column " << i);
    }
}

std::string MakeDecimalBytes(const NYql::NDecimal::TInt128 value) {
    std::string bytes(sizeof(value), '\0');
    std::memcpy(bytes.data(), &value, sizeof(value));
    return bytes;
}

arrow::util::string_view AsView(const std::string& value) {
    return { value.data(), value.size() };
}

}   // namespace

Y_UNIT_TEST_SUITE(ArrowReplaceKeyLogicalTypes) {
    Y_UNIT_TEST(NumericDecimalOrdering) {
        using namespace NYql::NDecimal;
        const TInt128 highWord = TInt128(1) << 64;
        const std::vector<TInt128> values = { -Inf(), -Inf() + 1, -highWord - 1, -highWord, -256, -1, 0, 1, 255, 256, highWord - 1, highWord,
            Inf() - 1, Inf(), Nan() };
        arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(16));
        UNIT_ASSERT(builder.AppendNull().ok());
        for (const auto value : values) {
            UNIT_ASSERT(builder.Append(MakeDecimalBytes(value)).ok());
        }

        std::shared_ptr<arrow::Array> array;
        UNIT_ASSERT(builder.Finish(&array).ok());
        const NScheme::TTypeInfo type(NScheme::TDecimalType(35, 0));
        for (size_t left = 0; left < values.size(); ++left) {
            const auto leftScalar = GetScalar(array, left + 1);
            for (size_t right = 0; right < values.size(); ++right) {
                const auto expected = values[left] <=> values[right];
                UNIT_ASSERT(NumericComparator.TypedCompare<true>(*array, left + 1, *array, right + 1, type) == expected);
                UNIT_ASSERT(NumericComparator.TypedCompare<false>(*array, left + 1, *array, right + 1, type) == expected);
                UNIT_ASSERT_VALUES_EQUAL(
                    ScalarCompare(leftScalar, GetScalar(array, right + 1), type, NumericComparator), int(expected > 0) - int(expected < 0));
            }
        }

        UNIT_ASSERT(NumericComparator.TypedCompare<false>(*array, 0, *array, 0, type) == std::partial_ordering::equivalent);
        UNIT_ASSERT(NumericComparator.TypedCompare<false>(*array, 0, *array, 1, type) == std::partial_ordering::less);
        UNIT_ASSERT(NumericComparator.TypedCompare<false>(*array, 1, *array, 0, type) == std::partial_ordering::greater);
        UNIT_ASSERT_VALUES_EQUAL(ScalarCompareNullable(nullptr, GetScalar(array, 1), type, NumericComparator), -1);
    }

    Y_UNIT_TEST(DecimalModeLeavesOtherLogicalTypesUnchanged) {
        const auto one = MakeDecimalBytes(1);
        const auto twoHundredFiftySix = MakeDecimalBytes(256);
        const auto minusOne = MakeDecimalBytes(-1);
        const auto zero = MakeDecimalBytes(0);
        const NScheme::TTypeInfo decimal(NScheme::TDecimalType(22, 9));
        UNIT_ASSERT(LegacyComparator.CompareValueNotNull(AsView(one), AsView(twoHundredFiftySix), decimal) > 0);
        UNIT_ASSERT(NumericComparator.CompareValueNotNull(AsView(one), AsView(twoHundredFiftySix), decimal) < 0);
        UNIT_ASSERT(LegacyComparator.CompareValueNotNull(AsView(minusOne), AsView(zero), decimal) > 0);
        UNIT_ASSERT(NumericComparator.CompareValueNotNull(AsView(minusOne), AsView(zero), decimal) < 0);
        for (const auto typeId : { NScheme::NTypeIds::Uuid, NScheme::NTypeIds::String, NScheme::NTypeIds::Utf8 }) {
            const NScheme::TTypeInfo type(typeId);
            UNIT_ASSERT(LegacyComparator.CompareValueNotNull(AsView(one), AsView(twoHundredFiftySix), type) > 0);
            UNIT_ASSERT(NumericComparator.CompareValueNotNull(AsView(one), AsView(twoHundredFiftySix), type) > 0);
            UNIT_ASSERT(LegacyComparator.IsCompatibleWith(NumericComparator, type));
        }

        UNIT_ASSERT(!LegacyComparator.IsCompatibleWith(NumericComparator, decimal));
        UNIT_ASSERT(LegacyComparator.IsCompatibleWith(LegacyComparator, decimal));
        UNIT_ASSERT(LegacyComparator.IsCompatibleWith(
            NumericComparator, TLogicalTypes{ NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), NScheme::TTypeInfo(NScheme::NTypeIds::Uuid) }));
        UNIT_ASSERT(!LegacyComparator.IsCompatibleWith(NumericComparator, MakeLogicalTypes()));
    }

    Y_UNIT_TEST(UnalignedDecimalViews) {
        const NScheme::TTypeInfo type(NScheme::TDecimalType(35, 2));
        const std::string left = "x" + MakeDecimalBytes(-256);
        const std::string right = "x" + MakeDecimalBytes(1);
        const arrow::util::string_view leftView(left.data() + 1, 16);
        const arrow::util::string_view rightView(right.data() + 1, 16);
        UNIT_ASSERT(NumericComparator.CompareValueNotNull(leftView, rightView, type) < 0);
        UNIT_ASSERT(NumericComparator.CompareValueNotNull(rightView, leftView, type) > 0);
        UNIT_ASSERT(NumericComparator.CompareValueNotNull(leftView, leftView, type) == std::partial_ordering::equivalent);
    }

    Y_UNIT_TEST(DecimalKeysKeepSelectedComparisonMode) {
        arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(16));
        UNIT_ASSERT(builder.Append(MakeDecimalBytes(1)).ok());
        UNIT_ASSERT(builder.Append(MakeDecimalBytes(256)).ok());
        std::shared_ptr<arrow::Array> values;
        UNIT_ASSERT(builder.Finish(&values).ok());
        const auto schema = arrow::schema({ arrow::field("decimal", values->type()) });
        const auto batch = arrow::RecordBatch::Make(schema, 2, { values });
        const TLogicalTypes types = { NScheme::TTypeInfo(NScheme::TDecimalType(22, 9)) };
        for (const auto mode : { EDecimalComparisonMode::Legacy, EDecimalComparisonMode::Numeric }) {
            const TComparator comparator(mode);
            const TReplaceKey one = TReplaceKey::FromBatch(batch, 0, types, comparator);
            const TReplaceKey twoHundredFiftySix = TReplaceKey::FromBatch(batch, 1, types, comparator);
            const bool numeric = mode == EDecimalComparisonMode::Numeric;
            UNIT_ASSERT_VALUES_EQUAL(one < twoHundredFiftySix, numeric);
            UNIT_ASSERT_VALUES_EQUAL(one.ToRaw() < twoHundredFiftySix.ToRaw(), numeric);
            UNIT_ASSERT_VALUES_EQUAL(one.GetView() < twoHundredFiftySix.GetView(), numeric);
            UNIT_ASSERT_VALUES_EQUAL(one.GetView().GetToStore() < twoHundredFiftySix.GetView().GetToStore(), numeric);
            UNIT_ASSERT_VALUES_EQUAL(TStoreReplaceKey(one) < TStoreReplaceKey(twoHundredFiftySix), numeric);
            UNIT_ASSERT_VALUES_EQUAL(TComparablePosition(one) < TComparablePosition(twoHundredFiftySix), numeric);
            UNIT_ASSERT_VALUES_EQUAL((IsSelfSorted<false, true>(batch, types, comparator)), numeric);
        }

        const TLogicalTypes uuidTypes = { NScheme::TTypeInfo(NScheme::NTypeIds::Uuid) };
        const TReplaceKey legacyOne = TReplaceKey::FromBatch(batch, 0, uuidTypes, LegacyComparator);
        const TReplaceKey numericTwoHundredFiftySix = TReplaceKey::FromBatch(batch, 1, uuidTypes, NumericComparator);
        UNIT_ASSERT(legacyOne > numericTwoHundredFiftySix);
        UNIT_ASSERT(TComparablePosition(numericTwoHundredFiftySix) < TComparablePosition(legacyOne));
    }

    Y_UNIT_TEST(OwningKeyAndBorrowedConversions) {
        const auto batch = MakeBatch();
        auto types = MakeLogicalTypes();
        const TReplaceKey key(std::make_shared<TArrayVec>(batch->columns()), 2, types, NumericComparator);
        types[0] = NScheme::TTypeInfo(NScheme::NTypeIds::Uuid);
        types.clear();
        CheckLogicalTypes(key.GetLogicalTypes(), MakeLogicalTypes());

        const auto raw = key.ToRaw();
        const auto view = key.GetView();
        UNIT_ASSERT(&raw.GetLogicalTypes() == &key.GetLogicalTypes());
        UNIT_ASSERT(&view.GetLogicalTypes() == &key.GetLogicalTypes());
        UNIT_ASSERT(raw == key);
        UNIT_ASSERT(view == key);
        UNIT_ASSERT(raw.GetComparator() == NumericComparator);
        UNIT_ASSERT(view.GetComparator() == NumericComparator);

        const TReplaceKey copy = key;
        const TStoreReplaceKey compacted(copy);
        CheckLogicalTypes(compacted.GetLogicalTypes(), MakeLogicalTypes());
        UNIT_ASSERT(compacted.GetComparator() == NumericComparator);
        UNIT_ASSERT_VALUES_EQUAL(compacted.GetPosition(), 0);
        UNIT_ASSERT_VALUES_EQUAL(compacted.Column(0).length(), 1);
        UNIT_ASSERT(compacted == key);
        UNIT_ASSERT_VALUES_EQUAL(key.Column(0).length(), 3);
    }

    Y_UNIT_TEST(StoredKeyOutlivesBorrowedTypes) {
        const auto stored = [] {
            const auto batch = MakeBatch();
            const auto types = MakeLogicalTypes();
            const TReplaceKeyView view(batch->columns(), 2, types, NumericComparator);
            UNIT_ASSERT(&view.GetLogicalTypes() == &types);
            return view.GetToStore();
        }();
        CheckLogicalTypes(stored.GetLogicalTypes(), MakeLogicalTypes());
        UNIT_ASSERT(stored.GetComparator() == NumericComparator);
        const auto batch = MakeBatch();
        const TReplaceKey expected = TReplaceKey::FromBatch(batch, 2, MakeLogicalTypes(), NumericComparator);
        UNIT_ASSERT(stored == expected);
    }

    Y_UNIT_TEST(FactoriesPreserveSelectedTypes) {
        const auto batch = MakeBatch();
        const auto types = MakeLogicalTypes();
        const TReplaceKey full = TReplaceKey::FromBatch(batch, 2, types, NumericComparator);
        CheckLogicalTypes(full.GetLogicalTypes(), types);

        const auto keySchema = arrow::schema({ batch->schema()->field(2), batch->schema()->field(0) });
        const TLogicalTypes keyTypes = { types[2], types[0] };
        const TReplaceKey selected = TReplaceKey::FromBatch(batch, keySchema, 2, keyTypes, NumericComparator);
        CheckLogicalTypes(selected.GetLogicalTypes(), keyTypes);
        UNIT_ASSERT(selected.ColumnPtr(0) == batch->column(2));
        UNIT_ASSERT(selected.ColumnPtr(1) == batch->column(0));

        const TReplaceKey scalarKey = TReplaceKey::FromScalar(GetScalar(batch->column(1), 2), types[1], NumericComparator);
        CheckLogicalTypes(scalarKey.GetLogicalTypes(), { types[1] });
        UNIT_ASSERT(scalarKey.GetComparator() == NumericComparator);
        UNIT_ASSERT(scalarKey.CompareColumnValue<true>(0, full, 1) == std::partial_ordering::equivalent);
        UNIT_ASSERT(ScalarCompare(TReplaceKey::ToScalar(scalarKey), GetScalar(batch->column(1), 2), types[1], NumericComparator) == 0);
    }

    Y_UNIT_TEST(ComparablePositionOwnsTypesAcrossChunks) {
        const auto batch = MakeBatch();
        auto types = MakeLogicalTypes();
        const TComparablePosition row(batch, 2, types, NumericComparator);
        std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
        for (size_t i = 0; i < types.size(); ++i) {
            const auto& values = batch->column(i);
            const i64 split = i == 0 ? 1 : 2;
            columns.emplace_back(std::make_shared<arrow::ChunkedArray>(TArrayVec{ values->Slice(0, split), values->Slice(split) }));
        }
        const auto table = arrow::Table::Make(batch->schema(), columns);
        const TComparablePosition fromTable(table, 2, types, NumericComparator);
        const TComparablePosition fromColumns(columns, 2, types, NumericComparator);
        const TReplaceKey key = TReplaceKey::FromBatch(batch, 2, types, NumericComparator);
        const TComparablePosition fromKey(key);
        const TComparablePosition fromView(key.GetView());
        types.clear();

        for (const auto* position : { &row, &fromTable, &fromColumns, &fromKey, &fromView }) {
            CheckLogicalTypes(position->GetLogicalTypes(), MakeLogicalTypes());
            UNIT_ASSERT(position->GetComparator() == NumericComparator);
            UNIT_ASSERT(position->Compare<true>(row) == std::partial_ordering::equivalent);
        }

        UNIT_ASSERT((IsSelfSorted<false, false>(batch, key.GetLogicalTypes(), NumericComparator)));
        UNIT_ASSERT((!IsSelfSorted<false, true>(batch, key.GetLogicalTypes(), NumericComparator)));
    }
}

}   // namespace NKikimr::NArrow
