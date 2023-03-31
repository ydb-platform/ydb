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
    TDqInputUnionStreamValue(TMemoryUsageInfo* memInfo, TVector<IDqInput::TPtr>&& inputs, TDqMeteringStats::TInputStatsMeter stats)
        : TComputationValue<TDqInputUnionStreamValue>(memInfo)
        , Inputs(std::move(inputs))
        , CurrentItemIndex(0)
        , Stats(stats)
    {}

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
        if (Stats) {
            Stats.Add(result);
        }
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
    TDqMeteringStats::TInputStatsMeter Stats;
};

class TDqInputMergeStreamValue : public TComputationValue<TDqInputMergeStreamValue> {
public:
    TDqInputMergeStreamValue(TMemoryUsageInfo* memInfo, TVector<IDqInput::TPtr>&& inputs,
        TVector<TSortColumnInfo>&& sortCols, TDqMeteringStats::TInputStatsMeter stats)
        : TComputationValue<TDqInputMergeStreamValue>(memInfo)
        , Inputs(std::move(inputs))
        , SortCols(std::move(sortCols))
        , Stats(stats)
        {
            CurrentBuffers.resize(Inputs.size());
            CurrentItemIndexes.reserve(Inputs.size());
            for (ui32 idx = 0; idx < Inputs.size(); ++idx) {
                CurrentItemIndexes.emplace_back(TUnboxedValuesIterator(*this, Inputs[idx], CurrentBuffers[idx]));
            }
            for (auto& sortCol : SortCols) {
                const auto typeId = sortCol.GetTypeId();
                TMaybe<EDataSlot> maybeDataSlot = FindDataSlot(typeId);
                YQL_ENSURE(maybeDataSlot, "Trying to compare columns with unknown type id: " << typeId);
                YQL_ENSURE(IsTypeSupportedInMergeCn(*maybeDataSlot), "Column '" << sortCol.Name <<
                    "' has unsupported type for Merge connection: " << *maybeDataSlot);
                SortColTypes[sortCol.Index] = *maybeDataSlot;
            }
        }

private:
    class TUnboxedValuesIterator {
    private:
        TUnboxedValueVector* Data = nullptr;
        IDqInput::TPtr Input;
        const TDqInputMergeStreamValue* Comparator = nullptr;
        ui32 CurrentIndex = 0;
    public:
        NUdf::EFetchStatus FindBuffer() {
            Data->clear();
            CurrentIndex = 0;
            if (Input->Pop(*Data)) {
                return NUdf::EFetchStatus::Ok;
            }

            return Input->IsFinished() ? NUdf::EFetchStatus::Finish : NUdf::EFetchStatus::Yield;
        }

        TUnboxedValuesIterator(const TDqInputMergeStreamValue& comparator, IDqInput::TPtr input, TUnboxedValueVector& data)
            : Data(&data)
            , Input(input)
            , Comparator(&comparator)
        {

        }

        bool IsYield() const {
            return CurrentIndex == Data->size();
        }

        bool operator<(const TUnboxedValuesIterator& item) const {
            return Comparator->CompareSortCols(GetValue(), item.GetValue()) > 0;
        }
        void operator++() {
            ++CurrentIndex;
            Y_VERIFY(CurrentIndex <= Data->size());
        }
        NKikimr::NUdf::TUnboxedValue& GetValue() {
            return (*Data)[CurrentIndex];
        }
        const NKikimr::NUdf::TUnboxedValue& GetValue() const {
            return (*Data)[CurrentIndex];
        }
    };

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
        if (Stats) {
            Stats.Add(result);
        }
        return NUdf::EFetchStatus::Ok;
    }

    NKikimr::NUdf::TUnboxedValue FindResult() {
        YQL_ENSURE(CurrentItemIndexes.size());
        std::pop_heap(CurrentItemIndexes.begin(), CurrentItemIndexes.end());
        auto& current = CurrentItemIndexes.back();
        Y_VERIFY(!current.IsYield());
        NKikimr::NUdf::TUnboxedValue res = current.GetValue();
        ++current;
        if (!current.IsYield()) {
            std::push_heap(CurrentItemIndexes.begin(), CurrentItemIndexes.end());
        }
        return res;
    }

    int CompareSortCols(const NKikimr::NUdf::TUnboxedValue& lhs, const NKikimr::NUdf::TUnboxedValue& rhs) const {
        int compRes = 0;
        for (auto sortCol = SortCols.begin(); sortCol != SortCols.end() && compRes == 0; ++sortCol) {
            auto lhsColValue = lhs.GetElement(sortCol->Index);
            auto rhsColValue = rhs.GetElement(sortCol->Index);
            auto it = SortColTypes.find(sortCol->Index);
            Y_VERIFY(it != SortColTypes.end());
            compRes = NKikimr::NMiniKQL::CompareValues(it->second,
                sortCol->Ascending, /* isOptional */ true, lhsColValue, rhsColValue);
        }
        return compRes;
    }

    NUdf::EFetchStatus CheckBuffers() {
        if (InitializationIndex >= CurrentItemIndexes.size()) {
            if (CurrentItemIndexes.size() && CurrentItemIndexes.back().IsYield()) {
                auto status = CurrentItemIndexes.back().FindBuffer();
                switch (status) {
                    case NUdf::EFetchStatus::Yield:
                        return status;
                    case NUdf::EFetchStatus::Finish:
                        CurrentItemIndexes.pop_back();
                        break;
                    case NUdf::EFetchStatus::Ok:
                        std::push_heap(CurrentItemIndexes.begin(), CurrentItemIndexes.end());
                        break;
                }
            }
        } else {
            while (InitializationIndex < CurrentItemIndexes.size()) {
                auto status = CurrentItemIndexes[InitializationIndex].FindBuffer();
                switch (status) {
                    case NUdf::EFetchStatus::Yield:
                        return status;
                    case NUdf::EFetchStatus::Finish:
                        std::swap(CurrentItemIndexes[InitializationIndex], CurrentItemIndexes.back());
                        CurrentItemIndexes.pop_back();
                        break;
                    case NUdf::EFetchStatus::Ok:
                        ++InitializationIndex;
                        break;
                }
            }
            std::make_heap(CurrentItemIndexes.begin(), CurrentItemIndexes.end());
        }
        if (CurrentItemIndexes.empty()) {
            return NUdf::EFetchStatus::Finish;
        }
        return NUdf::EFetchStatus::Ok;
    }

private:
    TVector<IDqInput::TPtr> Inputs;
    TVector<TSortColumnInfo> SortCols;
    TVector<TUnboxedValueVector> CurrentBuffers;
    TVector<TUnboxedValuesIterator> CurrentItemIndexes;
    ui32 InitializationIndex = 0;
    TMap<ui32, EDataSlot> SortColTypes;
    TDqMeteringStats::TInputStatsMeter Stats;
};

} // namespace

void TDqMeteringStats::TInputStatsMeter::Add(const NKikimr::NUdf::TUnboxedValue& val) {
    Stats->RowsConsumed += 1;
    if (InputType) {
        NYql::NDq::TDqDataSerializer::TEstimateSizeSettings settings;
        settings.DiscardUnsupportedTypes = true;
        settings.WithHeaders = false;

        Stats->BytesConsumed += Max<ui64>(TDqDataSerializer::EstimateSize(val, InputType, nullptr, settings), 8 /* billing size for count(*) */);
    }
}

NUdf::TUnboxedValue CreateInputUnionValue(TVector<IDqInput::TPtr>&& inputs,
    const NMiniKQL::THolderFactory& factory, TDqMeteringStats::TInputStatsMeter stats)
{
    return factory.Create<TDqInputUnionStreamValue>(std::move(inputs), stats);
}

NKikimr::NUdf::TUnboxedValue CreateInputMergeValue(TVector<IDqInput::TPtr>&& inputs,
    TVector<TSortColumnInfo>&& sortCols, const NKikimr::NMiniKQL::THolderFactory& factory, TDqMeteringStats::TInputStatsMeter stats)
{
    return factory.Create<TDqInputMergeStreamValue>(std::move(inputs), std::move(sortCols), stats);
}

} // namespace NYql::NDq
