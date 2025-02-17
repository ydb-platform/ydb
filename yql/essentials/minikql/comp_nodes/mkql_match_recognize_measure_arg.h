#pragma once

#include <yql/essentials/core/sql_types/match_recognize.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

using TMeasureInputColumnOrder = TMKQLVector<std::pair<NYql::NMatchRecognize::EMeasureInputDataSpecialColumns, size_t>>;

class TRowForMeasureValue final : public TComputationValue<TRowForMeasureValue> {
public:
    TRowForMeasureValue(
        TMemoryUsageInfo* memInfo,
        NUdf::TUnboxedValue inputRow,
        ui64 rowIndex,
        const TMeasureInputColumnOrder& columnOrder,
        const NUdf::TUnboxedValue& matchedVars,
        const TUnboxedValueVector& varNames,
        ui64 matchNumber);

    NUdf::TUnboxedValue GetElement(ui32 index) const final;

private:
    const NUdf::TUnboxedValue InputRow;
    const ui64 RowIndex;
    const TMeasureInputColumnOrder& ColumnOrder;
    const NUdf::TUnboxedValue& MatchedVars;
    const TUnboxedValueVector& VarNames;
    ui64 MatchNumber;
};

class TMeasureInputDataValue final : public TComputationValue<TMeasureInputDataValue> {
public:
    TMeasureInputDataValue(
        TMemoryUsageInfo* memInfo,
        const NUdf::TUnboxedValue& inputData,
        const TMeasureInputColumnOrder& columnOrder,
        const NUdf::TUnboxedValue& matchedVars,
        const TUnboxedValueVector& varNames,
        ui64 matchNumber);

    bool HasFastListLength() const final;
    ui64 GetListLength() const final;
    ui64 GetEstimatedListLength() const final;
    NUdf::TUnboxedValue GetListIterator() const final;
    bool HasListItems() const final;

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const final;

    ui64 GetDictLength() const final;
    NUdf::TUnboxedValue GetDictIterator() const final;
    NUdf::TUnboxedValue GetKeysIterator() const final;
    NUdf::TUnboxedValue GetPayloadsIterator() const final;
    bool Contains(const NUdf::TUnboxedValuePod& key) const final;
    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final;
    bool HasDictItems() const final;

private:
    const NUdf::TUnboxedValue InputData;
    const TMeasureInputColumnOrder& ColumnOrder;
    const NUdf::TUnboxedValue MatchedVars;
    const TUnboxedValueVector& VarNames;
    const ui64 MatchNumber;
};

} // namespace NKikimr::NMiniKQL::NMatchRecognize
