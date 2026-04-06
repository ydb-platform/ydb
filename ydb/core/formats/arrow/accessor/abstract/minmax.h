#pragma once

#include <arrow/datum.h>
#include <library/cpp/json/writer/json_value.h>
namespace NKikimr::NArrow::NAccessor {

struct TMinMax {
    arrow::StructScalar MinMax;
    TMinMax(arrow::StructScalar minmax)
        : MinMax(minmax) {
    }
    static TMinMax MakeNull(std::shared_ptr<arrow::DataType> type) {
        return *arrow::StructScalar::Make({ arrow::MakeNullScalar(type), arrow::MakeNullScalar(type) }, { "min", "max" }).ValueOrDie();
    }
    bool IsNull() const {
        auto min = Min();
        return min->Equals(arrow::MakeNullScalar(min->type));
    }

    std::shared_ptr<arrow::Scalar>& Min() {
        return MinMax.value[0];
    }

    const std::shared_ptr<arrow::Scalar>& Min() const {
        return MinMax.value[0];
    }

    std::shared_ptr<arrow::Scalar>& Max() {
        return MinMax.value[1];
    }

    const std::shared_ptr<arrow::Scalar>& Max() const {
        return MinMax.value[1];
    }

    void UniteWith(TMinMax other) {
        if (!other.IsNull()) {
            if (IsNull()) {
                *this = other;
            } else {
                Max() = std::max(Max(), other.Max());
                Min() = std::min(Min(), other.Min());
            }
        }
    }

    static TMinMax FromBinaryString(std::string_view string, const std::shared_ptr<arrow::DataType>& fieldType);

    static TMinMax FromArray(std::shared_ptr<arrow::Array> arr);
    static TMinMax FromArray(std::shared_ptr<arrow::ChunkedArray> arr);

    std::string ToBinaryString() const;

    NJson::TJsonValue Json() const;
};

}   // namespace NKikimr::NArrow::NAccessor
