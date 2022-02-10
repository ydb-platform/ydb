#include "dq_input_producer.h"

#include "dq_columns_resolve.h"

#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

namespace NYql::NDq {

using namespace NKikimr;
using namespace NMiniKQL;
using namespace NUdf;

namespace {

class TDqInputUnionStreamValue : public TComputationValue<TDqInputUnionStreamValue> {
public:
    TDqInputUnionStreamValue(TMemoryUsageInfo* memInfo, TVector<IDqInput::TPtr>&& inputs)
        : TComputationValue<TDqInputUnionStreamValue>(memInfo)
        , Inputs(std::move(inputs))
        , CurrentItemIndex(0) {}

private:
    NUdf::EFetchStatus Fetch(NKikimr::NUdf::TUnboxedValue& result) final {
        if (CurrentItemIndex >= CurrentBuffer.size()) {
            auto status = FindBuffer();
            switch (status) {
                case NUdf::EFetchStatus::Ok:
                    break;
                case NUdf::EFetchStatus::Finish:
                case NUdf::EFetchStatus::Yield:
                    return status;
            }
        }

        result = std::move(CurrentBuffer[CurrentItemIndex]);
        ++CurrentItemIndex;
        return NUdf::EFetchStatus::Ok;
    }

    NUdf::EFetchStatus FindBuffer() {
        bool allFinished = true;
        CurrentBuffer.clear();

        for (auto& input : Inputs) {
            if (input->Pop(CurrentBuffer)) {
                CurrentItemIndex = 0;
                return NUdf::EFetchStatus::Ok;
            }
            allFinished &= input->IsFinished();
        }

        return allFinished ? NUdf::EFetchStatus::Finish : NUdf::EFetchStatus::Yield;
    }

private:
    TVector<IDqInput::TPtr> Inputs;
    TUnboxedValueVector CurrentBuffer;
    ui64 CurrentItemIndex;
};

class TDqInputMergeStreamValue : public TComputationValue<TDqInputMergeStreamValue> {
public:
    TDqInputMergeStreamValue(TMemoryUsageInfo* memInfo, TVector<IDqInput::TPtr>&& inputs,
        TVector<TSortColumnInfo>&& sortCols)
        : TComputationValue<TDqInputMergeStreamValue>(memInfo)
        , Inputs(std::move(inputs))
        , SortCols(std::move(sortCols))
        , InputsSize(Inputs.size())
        {
            CurrentBuffers.resize(InputsSize);
            CurrentItemIndexes.resize(InputsSize, 0);
            Finished.resize(InputsSize, false);
            for (auto& sortCol : SortCols) {
                TMaybe<EDataSlot> maybeDataSlot = FindDataSlot(sortCol.TypeId);
                YQL_ENSURE(maybeDataSlot, "Trying to compare columns with unknown type id: " << sortCol.TypeId);
                YQL_ENSURE(IsTypeSupportedInMergeCn(*maybeDataSlot), "Column '" << sortCol.Name << 
                    "' has unsupported type for Merge connection: " << *maybeDataSlot);
                SortColTypes[sortCol.Index] = *maybeDataSlot;
            }
        }

private:
    NUdf::EFetchStatus Fetch(NKikimr::NUdf::TUnboxedValue& result) final {
        auto status = CheckBuffers();
        switch (status) {
            case NUdf::EFetchStatus::Ok:
                break;
            case NUdf::EFetchStatus::Finish:
            case NUdf::EFetchStatus::Yield:
                return status;
        }

        result = std::move(FindResult());
        return NUdf::EFetchStatus::Ok;
    }

    NKikimr::NUdf::TUnboxedValue FindResult() {
        NKikimr::NUdf::TUnboxedValue* res = nullptr;
        size_t chosenIndex = 0;
        for (size_t i = 0; i < InputsSize; ++i) {
            if (CurrentItemIndexes[i] >= CurrentBuffers[i].size()) {
                continue;
            }
            NKikimr::NUdf::TUnboxedValue &val = CurrentBuffers[i][CurrentItemIndexes[i]];
            if (!res || CompareSortCols(*res, val) > 0) {
                res = &val;
                chosenIndex = i;
            }
        }
        YQL_ENSURE(chosenIndex < InputsSize); 
        YQL_ENSURE(res); 
        ++CurrentItemIndexes[chosenIndex];
        return *res;
    }

    int CompareSortCols(NKikimr::NUdf::TUnboxedValue& lhs, NKikimr::NUdf::TUnboxedValue& rhs) {
        int compRes = 0;
        for (auto sortCol = SortCols.begin(); sortCol != SortCols.end() && compRes == 0; ++sortCol) {
            auto lhsColValue = lhs.GetElement(sortCol->Index);
            auto rhsColValue = rhs.GetElement(sortCol->Index);
            compRes = NKikimr::NMiniKQL::CompareValues(SortColTypes[sortCol->Index], 
                sortCol->Ascending, /* isOptional */ true, lhsColValue, rhsColValue);
        }
        return compRes;
    }

    NUdf::EFetchStatus CheckBuffers() {
        for (size_t i = 0; i < InputsSize; ++i) {
            if (CurrentItemIndexes[i] >= CurrentBuffers[i].size()) {
                auto status = FindBuffer(i);
                if (status == NUdf::EFetchStatus::Yield) {
                    return status;
                }
                Finished[i] = (status == NUdf::EFetchStatus::Finish);
            }
        }
        if (IsAllFinished()) {
            return NUdf::EFetchStatus::Finish;
        }
        return NUdf::EFetchStatus::Ok;
    }

    NUdf::EFetchStatus FindBuffer(size_t index) {
        CurrentBuffers[index].clear();

        if (Inputs[index]->Pop(CurrentBuffers[index])) {
            CurrentItemIndexes[index] = 0;
            return NUdf::EFetchStatus::Ok;
        }

        return Inputs[index]->IsFinished() ? NUdf::EFetchStatus::Finish : NUdf::EFetchStatus::Yield;
    }

    bool IsAllFinished() {
        bool allFinished = true;
        for (bool finished : Finished) {
            allFinished &= finished;
        }
        return allFinished;
    }

private:
    TVector<IDqInput::TPtr> Inputs;
    TVector<TSortColumnInfo> SortCols;
    size_t InputsSize;
    TVector<TUnboxedValueVector> CurrentBuffers;
    TVector<ui64> CurrentItemIndexes;
    TVector<bool> Finished;
    TMap<ui32, EDataSlot> SortColTypes;
};

} // namespace

NUdf::TUnboxedValue CreateInputUnionValue(TVector<IDqInput::TPtr>&& inputs,
    const NMiniKQL::THolderFactory& factory)
{
    return factory.Create<TDqInputUnionStreamValue>(std::move(inputs));
}

NKikimr::NUdf::TUnboxedValue CreateInputMergeValue(TVector<IDqInput::TPtr>&& inputs,
    TVector<TSortColumnInfo>&& sortCols, const NKikimr::NMiniKQL::THolderFactory& factory)
{
    return factory.Create<TDqInputMergeStreamValue>(std::move(inputs), std::move(sortCols));
}

} // namespace NYql::NDq
