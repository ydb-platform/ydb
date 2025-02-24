#include "mkql_match_recognize_measure_arg.h"

namespace NKikimr::NMiniKQL::NMatchRecognize {

TRowForMeasureValue::TRowForMeasureValue(
    TMemoryUsageInfo* memInfo,
    NUdf::TUnboxedValue inputRow,
    ui64 rowIndex,
    const TMeasureInputColumnOrder& columnOrder,
    const NUdf::TUnboxedValue& matchedVars,
    const TUnboxedValueVector& varNames,
    ui64 matchNumber)
: TComputationValue<TRowForMeasureValue>(memInfo)
, InputRow(inputRow)
, RowIndex(rowIndex)
, ColumnOrder(columnOrder)
, MatchedVars(matchedVars)
, VarNames(varNames)
, MatchNumber(matchNumber)
{}

NUdf::TUnboxedValue TRowForMeasureValue::GetElement(ui32 index) const {
    switch(ColumnOrder[index].first) {
        case NYql::NMatchRecognize::EMeasureInputDataSpecialColumns::Classifier: {
            auto varIterator = MatchedVars.GetListIterator();
            MKQL_ENSURE(varIterator, "Internal logic error");
            NUdf::TUnboxedValue var;
            size_t varIndex = 0;
            while(varIterator.Next(var)) {
                auto rangeIterator = var.GetListIterator();
                MKQL_ENSURE(varIterator, "Internal logic error");
                NUdf::TUnboxedValue range;
                while(rangeIterator.Next(range)) {
                    const auto from = range.GetElement(0).Get<ui64>();
                    const auto to = range.GetElement(1).Get<ui64>();
                    if (RowIndex >= from and RowIndex <= to) {
                        return VarNames[varIndex];
                    }
                }
                ++varIndex;
            }
            MKQL_ENSURE(MatchedVars.GetListLength() == varIndex, "Internal logic error");
            return MakeString("");
        }
        case NYql::NMatchRecognize::EMeasureInputDataSpecialColumns::MatchNumber:
            return NUdf::TUnboxedValuePod(MatchNumber);
        case NYql::NMatchRecognize::EMeasureInputDataSpecialColumns::Last: //Last corresponds to columns from the input table row
            return InputRow.GetElement(ColumnOrder[index].second);
    }
}

TMeasureInputDataValue::TMeasureInputDataValue(
    TMemoryUsageInfo* memInfo,
    const NUdf::TUnboxedValue& inputData,
    const TMeasureInputColumnOrder& columnOrder,
    const NUdf::TUnboxedValue& matchedVars,
    const TUnboxedValueVector& varNames,
    ui64 matchNumber)
: TComputationValue<TMeasureInputDataValue>(memInfo)
, InputData(inputData)
, ColumnOrder(columnOrder)
, MatchedVars(matchedVars)
, VarNames(varNames)
, MatchNumber(matchNumber)
{}

bool TMeasureInputDataValue::HasFastListLength() const {
    return true;
}

ui64 TMeasureInputDataValue::GetListLength() const {
    return GetDictLength();
}

ui64 TMeasureInputDataValue::GetEstimatedListLength() const {
    return GetListLength();
}

NUdf::TUnboxedValue TMeasureInputDataValue::GetListIterator() const {
    return GetPayloadsIterator();
}

bool TMeasureInputDataValue::HasListItems() const {
    return HasDictItems();
}

NUdf::IBoxedValuePtr TMeasureInputDataValue::ToIndexDictImpl(const NUdf::IValueBuilder& builder) const {
    Y_UNUSED(builder);
    return const_cast<TMeasureInputDataValue*>(this);
}

ui64 TMeasureInputDataValue::GetDictLength() const {
    return InputData.GetDictLength();
}

NUdf::TUnboxedValue TMeasureInputDataValue::GetDictIterator() const {
    return InputData.GetDictIterator();
}

NUdf::TUnboxedValue TMeasureInputDataValue::GetKeysIterator() const {
    return InputData.GetKeysIterator();
}

NUdf::TUnboxedValue TMeasureInputDataValue::GetPayloadsIterator() const {
    return InputData.GetPayloadsIterator();
}

bool TMeasureInputDataValue::Contains(const NUdf::TUnboxedValuePod& key) const {
    return InputData.Contains(key);
}

NUdf::TUnboxedValue TMeasureInputDataValue::Lookup(const NUdf::TUnboxedValuePod& key) const {
    return NUdf::TUnboxedValuePod{new TRowForMeasureValue(
        GetMemInfo(),
        InputData.Lookup(key),
        key.Get<ui64>(),
        ColumnOrder,
        MatchedVars,
        VarNames,
        MatchNumber
    )};
}

bool TMeasureInputDataValue::HasDictItems() const {
    return InputData.HasDictItems();
}

} // namespace NKikimr::NMiniKQL::NMatchRecognize
