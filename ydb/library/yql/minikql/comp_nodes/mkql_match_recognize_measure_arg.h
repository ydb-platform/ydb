#pragma once

#include "mkql_match_recognize_matched_vars.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/core/sql_types/match_recognize.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL::NMatchRecognize {

using NYql::NMatchRecognize::EMeasureInputDataSpecialColumns;

using TMeasureInputColumnOrder = std::vector<std::pair<EMeasureInputDataSpecialColumns, size_t>, TMKQLAllocator<std::pair<EMeasureInputDataSpecialColumns, size_t>>>;

//Input row augmented with lightweight special columns for calculating MEASURE lambdas
class TRowForMeasureValue: public TComputationValue<TRowForMeasureValue>
{
public:
    TRowForMeasureValue(
            TMemoryUsageInfo* memInfo,
            NUdf::TUnboxedValue inputRow,
            ui64 rowIndex,
            const TMeasureInputColumnOrder& columnOrder,
            const NUdf::TUnboxedValue& matchedVars,
            const TUnboxedValueVector& varNames,
            ui64 matchNumber
    )
            : TComputationValue<TRowForMeasureValue>(memInfo)
            , InputRow(inputRow)
            , RowIndex(rowIndex)
            , ColumnOrder(columnOrder)
            , MatchedVars(matchedVars)
            , VarNames(varNames)
            , MatchNumber(matchNumber)
    {}

    NUdf::TUnboxedValue GetElement(ui32 index) const override {
        switch(ColumnOrder[index].first) {
            case EMeasureInputDataSpecialColumns::Classifier: {
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
            case EMeasureInputDataSpecialColumns::MatchNumber:
                return NUdf::TUnboxedValuePod(MatchNumber);
            case EMeasureInputDataSpecialColumns::Last: //Last corresponds to columns from the input table row
                return InputRow.GetElement(ColumnOrder[index].second);
        }
    }
private:
    const NUdf::TUnboxedValue InputRow;
    const ui64 RowIndex;
    const TMeasureInputColumnOrder& ColumnOrder;
    const NUdf::TUnboxedValue& MatchedVars;
    const TUnboxedValueVector& VarNames;
    ui64 MatchNumber;
};

class TMeasureInputDataValue: public TComputationValue<TMeasureInputDataValue> {
    using Base = TComputationValue<TMeasureInputDataValue>;
public:
    TMeasureInputDataValue(TMemoryUsageInfo* memInfo,
                         const NUdf::TUnboxedValue& inputData,
                         const TMeasureInputColumnOrder& columnOrder,
                         const NUdf::TUnboxedValue& matchedVars,
                         const TUnboxedValueVector& varNames,
                         ui64 matchNumber)
    : Base(memInfo)
    , InputData(inputData)
    , ColumnOrder(columnOrder)
    , MatchedVars(matchedVars)
    , VarNames(varNames)
    , MatchNumber(matchNumber)
    {}

    bool HasFastListLength() const override {
        return InputData.HasFastListLength();
    }

    ui64 GetListLength() const override {
        return InputData.GetListLength();
    }

    //TODO https://st.yandex-team.ru/YQL-16508
    //NUdf::TUnboxedValue GetListIterator() const override;

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return const_cast<TMeasureInputDataValue*>(this);
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        auto inputRow = InputData.Lookup(key);
        return NUdf::TUnboxedValuePod{new TRowForMeasureValue(
            GetMemInfo(),
            inputRow,
            key.Get<ui64>(),
            ColumnOrder,
            MatchedVars,
            VarNames,
            MatchNumber
        )};
    }
private:
    const NUdf::TUnboxedValue InputData;
    const TMeasureInputColumnOrder& ColumnOrder;
    const NUdf::TUnboxedValue MatchedVars;
    const TUnboxedValueVector& VarNames;
    const ui64 MatchNumber;
};

}//namespace NKikimr::NMiniKQL::NMatchRecognize

