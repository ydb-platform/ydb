#include "dq_input_producer.h"

#include "dq_columns_resolve.h"

#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

namespace NYql::NDq {

using namespace NKikimr;
using namespace NMiniKQL;
using namespace NUdf;

namespace {

template<bool IsWide>
class TDqInputUnionStreamValue : public TComputationValue<TDqInputUnionStreamValue<IsWide>> {
    using TBase = TComputationValue<TDqInputUnionStreamValue<IsWide>>;
public:
    TDqInputUnionStreamValue(TMemoryUsageInfo* memInfo, TVector<IDqInput::TPtr>&& inputs, TDqMeteringStats::TInputStatsMeter stats)
        : TBase(memInfo)
        , Inputs(std::move(inputs))
        , Batch(Inputs.empty() ? nullptr : Inputs.front()->GetInputType())
        , Stats(stats)
    {}

private:
    NUdf::EFetchStatus Fetch(NKikimr::NUdf::TUnboxedValue& result) final {
        MKQL_ENSURE(!IsWide, "Using Fetch() on wide input");
        if (Batch.empty()) {
            auto status = FindBuffer();
            switch (status) {
                case NUdf::EFetchStatus::Ok:
                    break;
                case NUdf::EFetchStatus::Finish:
                case NUdf::EFetchStatus::Yield:
                    return status;
            }
        }

        result = std::move(*Batch.Head());
        Batch.Pop();

        if (Stats) {
            Stats.Add(result);
        }
        return NUdf::EFetchStatus::Ok;
    }

    NUdf::EFetchStatus WideFetch(NKikimr::NUdf::TUnboxedValue* result, ui32 width) final {
        YQL_ENSURE(IsWide, "Using WideFetch() on narrow input");
        if (Batch.empty()) {
            auto status = FindBuffer();
            switch (status) {
                case NUdf::EFetchStatus::Ok:
                    break;
                case NUdf::EFetchStatus::Finish:
                case NUdf::EFetchStatus::Yield:
                    return status;
            }
        }

        MKQL_ENSURE_S(Batch.Width() == width);
        NUdf::TUnboxedValue* head = Batch.Head();
        std::move(head, head + width, result);
        Batch.Pop();

        if (Stats) {
            Stats.Add(result, width);
        }
        return NUdf::EFetchStatus::Ok;
    }

    NUdf::EFetchStatus FindBuffer() {
        bool allFinished = true;
        Batch.clear();

        for (auto& input : Inputs) {
            if (input->Pop(Batch)) {
                return NUdf::EFetchStatus::Ok;
            }
            allFinished &= input->IsFinished();
        }

        return allFinished ? NUdf::EFetchStatus::Finish : NUdf::EFetchStatus::Yield;
    }

private:
    TVector<IDqInput::TPtr> Inputs;
    TUnboxedValueBatch Batch;
    TDqMeteringStats::TInputStatsMeter Stats;
};

template<bool IsWide>
class TDqInputMergeStreamValue : public TComputationValue<TDqInputMergeStreamValue<IsWide>> {
    using TBase = TComputationValue<TDqInputMergeStreamValue<IsWide>>;
public:
    TDqInputMergeStreamValue(TMemoryUsageInfo* memInfo, TVector<IDqInput::TPtr>&& inputs,
        TVector<TSortColumnInfo>&& sortCols, TDqMeteringStats::TInputStatsMeter stats)
        : TBase(memInfo)
        , Inputs(std::move(inputs))
        , SortCols(std::move(sortCols))
        , Stats(stats)
    {
        CurrentBuffers.resize(Inputs.size());
        CurrentItemIndexes.reserve(Inputs.size());
        for (ui32 idx = 0; idx < Inputs.size(); ++idx) {
            CurrentItemIndexes.emplace_back(TUnboxedValuesIterator<IsWide>(*this, Inputs[idx], CurrentBuffers[idx]));
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
    template<bool IsWideIter>
    class TUnboxedValuesIterator {
    private:
        TUnboxedValueBatch* Data = nullptr;
        IDqInput::TPtr Input;
        const TDqInputMergeStreamValue* Comparator = nullptr;
        ui32 Width_;
    public:
        NUdf::EFetchStatus FindBuffer() {
            Data->clear();
            if (Input->Pop(*Data)) {
                return NUdf::EFetchStatus::Ok;
            }

            return Input->IsFinished() ? NUdf::EFetchStatus::Finish : NUdf::EFetchStatus::Yield;
        }

        TUnboxedValuesIterator(const TDqInputMergeStreamValue<IsWideIter>& comparator, IDqInput::TPtr input, TUnboxedValueBatch& data)
            : Data(&data)
            , Input(input)
            , Comparator(&comparator)
            , Width_(data.IsWide() ? *data.Width() : 1u)
        {
        }

        bool IsYield() const {
            return Data->empty();
        }

        bool operator<(const TUnboxedValuesIterator& item) const {
            return Comparator->CompareSortCols(GetValue(), item.GetValue()) > 0;
        }

        ui32 Width() const {
            return Width_;
        }

        void Pop() {
            Data->Pop();
        }

        NKikimr::NUdf::TUnboxedValue* GetValue() {
            return Data->Head();
        }
        const NKikimr::NUdf::TUnboxedValue* GetValue() const {
            return Data->Head();
        }
    };

    NUdf::EFetchStatus Fetch(NKikimr::NUdf::TUnboxedValue& result) final {
        YQL_ENSURE(!IsWide, "Using Fetch() on wide input");
        auto status = CheckBuffers();
        switch (status) {
            case NUdf::EFetchStatus::Ok:
                break;
            case NUdf::EFetchStatus::Finish:
            case NUdf::EFetchStatus::Yield:
                return status;
        }

        CopyResult(&result, 1);
        if (Stats) {
            Stats.Add(result);
        }
        return NUdf::EFetchStatus::Ok;
    }

    NUdf::EFetchStatus WideFetch(NKikimr::NUdf::TUnboxedValue* result, ui32 width) final {
        YQL_ENSURE(IsWide, "Using WideFetch() on narrow input");
        auto status = CheckBuffers();
        switch (status) {
            case NUdf::EFetchStatus::Ok:
                break;
            case NUdf::EFetchStatus::Finish:
            case NUdf::EFetchStatus::Yield:
                return status;
        }

        YQL_ENSURE(!Inputs.empty() && *Inputs.front()->GetInputWidth() == width);
        CopyResult(result, width);
        if (Stats) {
            Stats.Add(result, width);
        }
        return NUdf::EFetchStatus::Ok;
    }


    void CopyResult(NKikimr::NUdf::TUnboxedValue* dst, ui32 width) {
        YQL_ENSURE(CurrentItemIndexes.size());
        std::pop_heap(CurrentItemIndexes.begin(), CurrentItemIndexes.end());
        auto& current = CurrentItemIndexes.back();
        Y_VERIFY(!current.IsYield());
        YQL_ENSURE(width == current.Width());

        NKikimr::NUdf::TUnboxedValue* res = current.GetValue();
        std::move(res, res + width, dst);
        current.Pop();
        if (!current.IsYield()) {
            std::push_heap(CurrentItemIndexes.begin(), CurrentItemIndexes.end());
        }
    }

    int CompareSortCols(const NKikimr::NUdf::TUnboxedValue* lhs, const NKikimr::NUdf::TUnboxedValue* rhs) const {
        int compRes = 0;
        for (auto sortCol = SortCols.begin(); sortCol != SortCols.end() && compRes == 0; ++sortCol) {
            NKikimr::NUdf::TUnboxedValue lhsColValue;
            NKikimr::NUdf::TUnboxedValue rhsColValue;
            if constexpr (IsWide) {
                lhsColValue = *(lhs + sortCol->Index);
                rhsColValue = *(rhs + sortCol->Index);
            } else {
                lhsColValue = lhs->GetElement(sortCol->Index);
                rhsColValue = rhs->GetElement(sortCol->Index);
            }
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
    TVector<TUnboxedValueBatch> CurrentBuffers;
    TVector<TUnboxedValuesIterator<IsWide>> CurrentItemIndexes;
    ui32 InitializationIndex = 0;
    TMap<ui32, EDataSlot> SortColTypes;
    TDqMeteringStats::TInputStatsMeter Stats;
};

bool IsWideInputs(const TVector<IDqInput::TPtr>& inputs) {
    NKikimr::NMiniKQL::TType* type = nullptr;
    bool isWide = false;
    for (auto& input : inputs) {
        if (!type) {
            type = input->GetInputType();
            isWide = input->GetInputWidth().Defined();
        } else {
            YQL_ENSURE(type->IsSameType(*input->GetInputType()));
        }
    }
    return isWide;
}

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

void TDqMeteringStats::TInputStatsMeter::Add(const NKikimr::NUdf::TUnboxedValue* row, ui32 width) {
    Stats->RowsConsumed += 1;
    if (InputType) {
        YQL_ENSURE(InputType->IsMulti());
        auto multiType = static_cast<const TMultiType*>(InputType);
        YQL_ENSURE(width == multiType->GetElementsCount());
        
        NYql::NDq::TDqDataSerializer::TEstimateSizeSettings settings;
        settings.DiscardUnsupportedTypes = true;
        settings.WithHeaders = false;

        ui64 size = 0;
        for (ui32 i = 0; i < multiType->GetElementsCount(); ++i) {
            size += TDqDataSerializer::EstimateSize(row[i], multiType->GetElementType(i), nullptr, settings);
        }

        Stats->BytesConsumed += Max<ui64>(size, 8 /* billing size for count(*) */);
    }
}

NUdf::TUnboxedValue CreateInputUnionValue(TVector<IDqInput::TPtr>&& inputs,
    const NMiniKQL::THolderFactory& factory, TDqMeteringStats::TInputStatsMeter stats)
{
    if (IsWideInputs(inputs)) {
        return factory.Create<TDqInputUnionStreamValue<true>>(std::move(inputs), stats);
    }
    return factory.Create<TDqInputUnionStreamValue<false>>(std::move(inputs), stats);
}

NKikimr::NUdf::TUnboxedValue CreateInputMergeValue(TVector<IDqInput::TPtr>&& inputs,
    TVector<TSortColumnInfo>&& sortCols, const NKikimr::NMiniKQL::THolderFactory& factory, TDqMeteringStats::TInputStatsMeter stats)
{
    if (IsWideInputs(inputs)) {
        return factory.Create<TDqInputMergeStreamValue<true>>(std::move(inputs), std::move(sortCols), stats);
    }
    return factory.Create<TDqInputMergeStreamValue<false>>(std::move(inputs), std::move(sortCols), stats);
}

} // namespace NYql::NDq
