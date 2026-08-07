#include "mkql_match_recognize_measure_arg.h"

#include <utility>

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
    , InputRow_(std::move(inputRow))
    , RowIndex_(rowIndex)
    , ColumnOrder_(columnOrder)
    , MatchedVars_(matchedVars)
    , VarNames_(varNames)
    , MatchNumber_(matchNumber)
{
}

NUdf::TUnboxedValue TRowForMeasureValue::GetElement(ui32 index) const {
    switch (ColumnOrder_[index].first) {
        case NYql::NMatchRecognize::EMeasureInputDataSpecialColumns::Classifier: {
            auto varIterator = MatchedVars_.GetListIterator();
            MKQL_ENSURE(varIterator, "Internal logic error");
            NUdf::TUnboxedValue var;
            size_t varIndex = 0;
            while (varIterator.Next(var)) {
                auto rangeIterator = var.GetListIterator();
                MKQL_ENSURE(varIterator, "Internal logic error");
                NUdf::TUnboxedValue range;
                while (rangeIterator.Next(range)) {
                    const auto from = range.GetElement(0).Get<ui64>();
                    const auto to = range.GetElement(1).Get<ui64>();
                    if (RowIndex_ >= from and RowIndex_ <= to) {
                        return VarNames_[varIndex];
                    }
                }
                ++varIndex;
            }
            MKQL_ENSURE(MatchedVars_.GetListLength() == varIndex, "Internal logic error");
            return MakeString("");
        }
        case NYql::NMatchRecognize::EMeasureInputDataSpecialColumns::MatchNumber:
            return NUdf::TUnboxedValuePod(MatchNumber_);
        case NYql::NMatchRecognize::EMeasureInputDataSpecialColumns::Last: // Last corresponds to columns from the input table row
            return InputRow_.GetElement(ColumnOrder_[index].second);
    }
}

TMeasureInputDataValue::TMeasureInputDataValue(
    TMemoryUsageInfo* memInfo,
    NUdf::TUnboxedValue inputData,
    const TMeasureInputColumnOrder& columnOrder,
    NUdf::TUnboxedValue matchedVars,
    const TUnboxedValueVector& varNames,
    ui64 matchNumber)
    : TComputationValue<TMeasureInputDataValue>(memInfo)
    , InputData_(std::move(inputData))
    , ColumnOrder_(columnOrder)
    , MatchedVars_(std::move(matchedVars))
    , VarNames_(varNames)
    , MatchNumber_(matchNumber)
{
}

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
    return InputData_.GetDictLength();
}

NUdf::TUnboxedValue TMeasureInputDataValue::GetDictIterator() const {
    return InputData_.GetDictIterator();
}

NUdf::TUnboxedValue TMeasureInputDataValue::GetKeysIterator() const {
    return InputData_.GetKeysIterator();
}

NUdf::TUnboxedValue TMeasureInputDataValue::GetPayloadsIterator() const {
    return InputData_.GetPayloadsIterator();
}

bool TMeasureInputDataValue::Contains(const NUdf::TUnboxedValuePod& key) const {
    return InputData_.Contains(key);
}

NUdf::TUnboxedValue TMeasureInputDataValue::Lookup(const NUdf::TUnboxedValuePod& key) const {
    return NUdf::TUnboxedValuePod{new TRowForMeasureValue(
        GetMemInfo(),
        InputData_.Lookup(key),
        key.Get<ui64>(),
        ColumnOrder_,
        MatchedVars_,
        VarNames_,
        MatchNumber_)};
}

bool TMeasureInputDataValue::HasDictItems() const {
    return InputData_.HasDictItems();
}

} // namespace NKikimr::NMiniKQL::NMatchRecognize
