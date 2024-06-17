#include "calcer.h"
#include "xx_hash.h"
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/library/services/services.pb.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <ydb/library/actors/core/log.h>
#include <util/string/join.h>

namespace NKikimr::NArrow::NHash {

void TXX64::AppendField(const std::shared_ptr<arrow::Scalar>& scalar, NXX64::TStreamStringHashCalcer& hashCalcer) {
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

void TXX64::AppendField(const std::shared_ptr<arrow::Array>& array, const int row, NArrow::NHash::NXX64::TStreamStringHashCalcer& hashCalcer) {
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

}
