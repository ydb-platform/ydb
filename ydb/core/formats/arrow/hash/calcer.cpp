#include "calcer.h"

#include <ydb/core/formats/arrow/switch/switch_type.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/hash/xx_hash.h>
#include <ydb/library/services/services.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <util/string/join.h>

namespace NKikimr::NArrow::NHash {

namespace {
template <class TStreamCalcer>
void AppendFieldImpl(const std::shared_ptr<arrow::Scalar>& scalar, TStreamCalcer& hashCalcer) {
    AFL_VERIFY(scalar);
    NArrow::SwitchType(scalar->type->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using T = typename TWrap::T;
        using TScalar = typename arrow::TypeTraits<T>::ScalarType;

        auto& typedScalar = static_cast<const TScalar&>(*scalar);
        if constexpr (arrow::has_string_view<T>()) {
            hashCalcer.Update(reinterpret_cast<const ui8*>(typedScalar.value->data()), typedScalar.value->size());
        } else if constexpr (arrow::has_c_type<T>()) {
            hashCalcer.Update(reinterpret_cast<const ui8*>(typedScalar.data()), sizeof(typedScalar.value));
        } else {
            static_assert(arrow::is_decimal_type<T>());
        }
        return true;
    });
}

template <class TStreamCalcer>
void AppendFieldImpl(const std::shared_ptr<arrow::Array>& array, const int row, TStreamCalcer& hashCalcer) {
    NArrow::SwitchType(array->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using T = typename TWrap::T;
        using TArray = typename arrow::TypeTraits<T>::ArrayType;

        if (!array->IsNull(row)) {
            auto& typedArray = static_cast<const TArray&>(*array);
            auto value = typedArray.GetView(row);
            if constexpr (arrow::has_string_view<T>()) {
                hashCalcer.Update((const ui8*)value.data(), value.size());
            } else if constexpr (arrow::has_c_type<T>()) {
                hashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
            } else {
                static_assert(arrow::is_decimal_type<T>());
            }
        }
        return true;
    });
}

}   // namespace

ui64 TXX64::CalcSimple(const void* data, const ui32 dataSize, const ui64 seed) {
    return XXH3_64bits_withSeed((const ui8*)data, dataSize, seed);
}

ui64 TXX64::CalcSimple(const std::string_view data, const ui64 seed) {
    return CalcSimple(data.data(), data.size(), seed);
}

ui64 TXX64::CalcForScalar(const std::shared_ptr<arrow::Scalar>& scalar, const ui64 seed) {
    AFL_VERIFY(scalar);
    ui64 result = 0;
    NArrow::SwitchType(scalar->type->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using T = typename TWrap::T;
        using TScalar = typename arrow::TypeTraits<T>::ScalarType;

        auto& typedScalar = static_cast<const TScalar&>(*scalar);
        if constexpr (arrow::has_string_view<T>()) {
            result = CalcSimple(typedScalar.value->data(), typedScalar.value->size(), seed);
        } else if constexpr (arrow::has_c_type<T>()) {
            result = CalcSimple(typedScalar.data(), sizeof(typedScalar.value), seed);
        } else {
            static_assert(arrow::is_decimal_type<T>());
            AFL_VERIFY(false);
        }
        return true;
    });
    return result;
}

void TXX64::AppendField(const std::shared_ptr<arrow::Scalar>& scalar, NXX64::TStreamStringHashCalcer& hashCalcer) {
    AppendFieldImpl(scalar, hashCalcer);
}

void TXX64::AppendField(const std::shared_ptr<arrow::Scalar>& scalar, NXX64::TStreamStringHashCalcer_H3& hashCalcer) {
    AppendFieldImpl(scalar, hashCalcer);
}

void TXX64::AppendField(const std::shared_ptr<arrow::Array>& array, const int row, NArrow::NHash::NXX64::TStreamStringHashCalcer& hashCalcer) {
    AppendFieldImpl(array, row, hashCalcer);
}

void TXX64::AppendField(
    const std::shared_ptr<arrow::Array>& array, const int row, NArrow::NHash::NXX64::TStreamStringHashCalcer_H3& hashCalcer) {
    AppendFieldImpl(array, row, hashCalcer);
}

std::optional<std::vector<ui64>> TXX64::Execute(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    std::vector<std::shared_ptr<arrow::Array>> columns = GetColumns(batch);
    if (columns.empty()) {
        return {};
    }

    std::vector<ui64> out(batch->num_rows());
    NXX64::TStreamStringHashCalcer hashCalcer(Seed);

    for (int row = 0; row < batch->num_rows(); ++row) {
        hashCalcer.Start();
        for (auto& column : columns) {
            AppendField(column, row, hashCalcer);
        }
        out[row] = hashCalcer.Finish();
    }

    return out;
}

TXX64::TXX64(const std::vector<TString>& columnNames, const ENoColumnPolicy noColumnPolicy, const ui64 seed)
    : Seed(seed)
    , ColumnNames(columnNames)
    , NoColumnPolicy(noColumnPolicy) {
    Y_ABORT_UNLESS(ColumnNames.size() >= 1);
}

TXX64::TXX64(const std::vector<std::string>& columnNames, const ENoColumnPolicy noColumnPolicy, const ui64 seed)
    : Seed(seed)
    , ColumnNames(columnNames.begin(), columnNames.end())
    , NoColumnPolicy(noColumnPolicy) {
    Y_ABORT_UNLESS(ColumnNames.size() >= 1);
}

ui64 TXX64::CalcHash(const std::shared_ptr<arrow::Scalar>& scalar) {
    NXX64::TStreamStringHashCalcer calcer(0);
    calcer.Start();
    AppendField(scalar, calcer);
    return calcer.Finish();
}

namespace {

template <class TDataContainer>
bool BuildHashUI64Impl(std::shared_ptr<TDataContainer>& batch, const std::vector<std::string>& fieldNames, const std::string& hashFieldName) {
    if (fieldNames.size() == 0) {
        return false;
    }
    Y_ABORT_UNLESS(!batch->GetColumnByName(hashFieldName));
    if (fieldNames.size() == 1) {
        auto column = batch->GetColumnByName(fieldNames.front());
        if (!column) {
            AFL_WARN(NKikimrServices::ARROW_HELPER)("event", "cannot_build_hash")("reason", "field_not_found")("field_name", fieldNames.front());
            return false;
        }
        Y_ABORT_UNLESS(column);
        if (column->type()->id() == arrow::Type::UINT64 || column->type()->id() == arrow::Type::UINT32 ||
            column->type()->id() == arrow::Type::INT64 || column->type()->id() == arrow::Type::INT32) {
            batch = TStatusValidator::GetValid(
                batch->AddColumn(batch->num_columns(), std::make_shared<arrow::Field>(hashFieldName, column->type()), column));
            return true;
        }
    }
    std::shared_ptr<arrow::Array> hashColumn =
        NArrow::NHash::TXX64(fieldNames, NArrow::NHash::TXX64::ENoColumnPolicy::Verify, 34323543).ExecuteToArray(batch);
    batch = NAdapter::TDataBuilderPolicy<TDataContainer>::AddColumn(
        batch, std::make_shared<arrow::Field>(hashFieldName, hashColumn->type()), hashColumn);
    return true;
}

}   // namespace

bool THashConstructor::BuildHashUI64(
    std::shared_ptr<arrow::Table>& batch, const std::vector<std::string>& fieldNames, const std::string& hashFieldName) {
    return BuildHashUI64Impl(batch, fieldNames, hashFieldName);
}

bool THashConstructor::BuildHashUI64(
    std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& fieldNames, const std::string& hashFieldName) {
    return BuildHashUI64Impl(batch, fieldNames, hashFieldName);
}

}   // namespace NKikimr::NArrow::NHash
