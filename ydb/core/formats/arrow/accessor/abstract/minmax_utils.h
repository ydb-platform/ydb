#pragma once

#include <ydb/library/formats/arrow/switch/compare.h>

#include <arrow/datum.h>
#include <library/cpp/json/writer/json_value.h>
namespace NKikimr::NArrow::NAccessor {

struct TMinMax {
private:
    TComparator::EComparisonOrder ComparisonOrder = TComparator::EComparisonOrder::Default;

public:
    arrow::StructScalar MinMax;
    TMinMax(arrow::StructScalar minmax)
        : MinMax(minmax) 
    {
    }
    
    static TMinMax MakeNull(std::shared_ptr<arrow::DataType> type) {
        return *arrow::StructScalar::Make({ arrow::MakeNullScalar(type), arrow::MakeNullScalar(type) }, { "min", "max" }).ValueOrDie();
    }

    bool IsNull() const;

    std::shared_ptr<arrow::DataType> ElementType() const;

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

    void UniteWith(const TMinMax& other, const NScheme::TTypeInfo& logicalType);

    std::partial_ordering CompareWithMin(const std::shared_ptr<arrow::Scalar>& value, const NScheme::TTypeInfo& logicalType) const;
    std::partial_ordering CompareWithMax(const std::shared_ptr<arrow::Scalar>& value, const NScheme::TTypeInfo& logicalType) const;

    static TMinMax FromBinaryString(std::string_view string, const std::shared_ptr<arrow::DataType>& fieldType);

    static TMinMax Compute(const std::shared_ptr<arrow::Array>& arr, const NScheme::TTypeInfo& logicalType);
    static TMinMax Compute(const std::shared_ptr<arrow::ChunkedArray>& arr, const NScheme::TTypeInfo& logicalType);

    TString ToBinaryString() const;
};

}   // namespace NKikimr::NArrow::NAccessor
