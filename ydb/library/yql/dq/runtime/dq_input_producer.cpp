#include "dq_input_producer.h"

#include "dq_columns_resolve.h"

#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

#include <ydb/library/yql/public/udf/arrow/args_dechunker.h>
#include <ydb/library/yql/public/udf/arrow/memory_pool.h>

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
    TDqInputUnionStreamValue(TMemoryUsageInfo* memInfo, const NKikimr::NMiniKQL::TType* type, TVector<IDqInput::TPtr>&& inputs, TDqMeteringStats::TInputStatsMeter stats)
        : TBase(memInfo)
        , Inputs(std::move(inputs))
        , Alive(Inputs.size())
        , Batch(type)
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
        Batch.clear();

        auto startIndex = Index++;
        size_t i = 0;

        while (i < Alive) {
            auto currentIndex = (startIndex + i) % Alive;
            auto& input = Inputs[currentIndex];
            if (input->Pop(Batch)) {
                return NUdf::EFetchStatus::Ok;
            }
            if (input->IsFinished()) {
                std::swap(Inputs[currentIndex], Inputs[Alive - 1]);
                --Alive;
            } else {
                ++i;
            }
        }

        return Alive == 0 ? NUdf::EFetchStatus::Finish : NUdf::EFetchStatus::Yield;
    }

private:
    TVector<IDqInput::TPtr> Inputs;
    size_t Alive;
    size_t Index = 0;
    TUnboxedValueBatch Batch;
    TDqMeteringStats::TInputStatsMeter Stats;
};

template<bool IsWide>
class TDqInputMergeStreamValue : public TComputationValue<TDqInputMergeStreamValue<IsWide>> {
    using TBase = TComputationValue<TDqInputMergeStreamValue<IsWide>>;
public:
    TDqInputMergeStreamValue(TMemoryUsageInfo* memInfo, const NKikimr::NMiniKQL::TType* type, TVector<IDqInput::TPtr>&& inputs,
        TVector<TSortColumnInfo>&& sortCols, TDqMeteringStats::TInputStatsMeter stats)
        : TBase(memInfo)
        , Inputs(std::move(inputs))
        , Width(type->IsMulti() ? static_cast<const NMiniKQL::TMultiType*>(type)->GetElementsCount() : TMaybe<ui32>())
        , SortCols(std::move(sortCols))
        , Stats(stats)
    {
        YQL_ENSURE(!IsWide ^ Width.Defined());
        CurrentBuffers.reserve(Inputs.size());
        CurrentItemIndexes.reserve(Inputs.size());
        for (ui32 idx = 0; idx < Inputs.size(); ++idx) {
            CurrentBuffers.emplace_back(Inputs[idx]->GetInputType());
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

        YQL_ENSURE(*Width == width);
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
        Y_ABORT_UNLESS(!current.IsYield());
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
            Y_ABORT_UNLESS(it != SortColTypes.end());
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
    const TMaybe<ui32> Width;
    TVector<TSortColumnInfo> SortCols;
    TVector<TUnboxedValueBatch> CurrentBuffers;
    TVector<TUnboxedValuesIterator<IsWide>> CurrentItemIndexes;
    ui32 InitializationIndex = 0;
    TMap<ui32, EDataSlot> SortColTypes;
    TDqMeteringStats::TInputStatsMeter Stats;
};

TVector<NKikimr::NMiniKQL::TType*> ExtractBlockItemTypes(const NKikimr::NMiniKQL::TType* type) {
    TVector<NKikimr::NMiniKQL::TType*> result;

    YQL_ENSURE(type->IsMulti());
    auto multiType = static_cast<const NKikimr::NMiniKQL::TMultiType*>(type);
    YQL_ENSURE(multiType->GetElementsCount() > 0);

    for (ui32 i = 0; i < multiType->GetElementsCount(); ++i) {
        auto itemType = multiType->GetElementType(i);
        YQL_ENSURE(itemType->IsBlock());
        auto blockType = static_cast<const NKikimr::NMiniKQL::TBlockType*>(itemType);
        const bool isScalar = blockType->GetShape() == NKikimr::NMiniKQL::TBlockType::EShape::Scalar;

        if (i + 1 == multiType->GetElementsCount()) {
            YQL_ENSURE(isScalar);
        } else {
            result.emplace_back(isScalar ? nullptr : blockType->GetItemType());
        }
    }
    return result;
}

TVector<std::unique_ptr<IBlockReader>> MakeReaders(const TVector<NKikimr::NMiniKQL::TType*> itemTypes) {
    TVector<std::unique_ptr<IBlockReader>> result;
    for (auto& itemType : itemTypes) {
        if (itemType) {
            result.emplace_back(MakeBlockReader(TTypeInfoHelper(), itemType));
        } else {
            result.emplace_back();
        }
    }
    return result;
}

TVector<std::unique_ptr<IArrayBuilder>> MakeBuilders(ui64 blockLen, const TVector<NKikimr::NMiniKQL::TType*> itemTypes) {
    TVector<std::unique_ptr<IArrayBuilder>> result;
    TTypeInfoHelper helper;
    for (auto& itemType : itemTypes) {
        if (itemType) {
            // TODO: pass memory pool
            // TODO: IPgBuilder
            YQL_ENSURE(!itemType->IsPg(), "pg types are not supported yet");
            result.emplace_back(MakeArrayBuilder(helper, itemType, *NYql::NUdf::GetYqlMemoryPool(), blockLen, nullptr));
        } else {
            result.emplace_back();
        }
    }
    return result;
}

TVector<IBlockItemComparator::TPtr> MakeComparators(const TVector<TSortColumnInfo>& sortCols,
    const TVector<NKikimr::NMiniKQL::TType*> itemTypes)
{
    TVector<IBlockItemComparator::TPtr> result;
    TBlockTypeHelper helper;
    for (auto& sortCol : sortCols) {
        YQL_ENSURE(sortCol.Index < itemTypes.size());

        auto itemType = itemTypes[sortCol.Index];
        YQL_ENSURE(itemType);
        result.emplace_back(helper.MakeComparator(itemType));
    }
    return result;
}

class TDqInputMergeBlockStreamValue : public TComputationValue<TDqInputMergeBlockStreamValue> {
    using TBase = TComputationValue<TDqInputMergeBlockStreamValue>;
public:
    TDqInputMergeBlockStreamValue(TMemoryUsageInfo* memInfo, const NKikimr::NMiniKQL::TType* type, TVector<IDqInput::TPtr>&& inputs,
        TVector<TSortColumnInfo>&& sortCols, const NKikimr::NMiniKQL::THolderFactory& factory, TDqMeteringStats::TInputStatsMeter stats)
        : TBase(memInfo)
        , SortCols_(std::move(sortCols))
        , ItemTypes_(ExtractBlockItemTypes(type))
        , MaxOutputBlockLen_(CalcMaxBlockLength(ItemTypes_.begin(), ItemTypes_.end(), TTypeInfoHelper()))
        , Comparators_(MakeComparators(SortCols_, ItemTypes_))
        , Builders_(MakeBuilders(MaxOutputBlockLen_, ItemTypes_))
        , Factory_(factory)
        , Stats_(stats)
    {
        YQL_ENSURE(MaxOutputBlockLen_ > 0);
        InputData_.reserve(inputs.size());
        for (auto& input : inputs) {
            InputData_.emplace_back(std::move(input), this);
        }
    }

private:
    struct TDqInputBatch : private TMoveOnly {
        explicit TDqInputBatch(IDqInput::TPtr&& input, const TDqInputMergeBlockStreamValue* parent)
            : Input_(std::move(input))
            , FetchedValues_(Input_->GetInputType())
            , Readers_(MakeReaders(parent->ItemTypes_))
            , Parent_(parent)
        {
            YQL_ENSURE(Parent_);
            CurrentRow_.reserve(parent->ItemTypes_.size());
        }

        ui64 CurrentIndex() const {
            return CurrBlockIndex_;
        }

        ui64 BlockLen() const {
            return BlockLen_;
        }

        bool IsEmpty() const {
            return CurrBlockIndex_ >= BlockLen_;
        }

        void NextRow() {
            Y_DEBUG_ABORT_UNLESS(!IsEmpty());
            ++CurrBlockIndex_;
        }

        NUdf::EFetchStatus FetchNext() {
            if (IsFinished_) {
                return NUdf::EFetchStatus::Finish;
            }

            const ui32 width = Parent_->ItemTypes_.size();
            while (IsEmpty()) {
                while (FetchedValues_.empty()) {
                    if (!Input_->Pop(FetchedValues_)) {
                        if (Input_->IsFinished()) {
                            IsFinished_ = true;
                            return NUdf::EFetchStatus::Finish;
                        }
                        return NUdf::EFetchStatus::Yield;
                    }
                }
                NUdf::TUnboxedValue* values = FetchedValues_.Head();
                CurrentRow_.clear();
                for (ui32 i = 0; i < width; ++i) {
                    CurrentRow_.emplace_back(TArrowBlock::From(values[i]).GetDatum());
                }
                CurrBlockIndex_ = 0;
                BlockLen_ = TArrowBlock::From(values[width]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
                FetchedValues_.Pop();
            }
            return NUdf::EFetchStatus::Ok;
        }

        const TDqInputMergeBlockStreamValue& Parent() const {
            return *Parent_;
        }

        TBlockItem GetColumnItem(ui32 columnIndex, ui64 blockIndex) const {
            Y_DEBUG_ABORT_UNLESS(columnIndex < CurrentRow_.size());
            auto& datum = CurrentRow_[columnIndex];
            Y_DEBUG_ABORT_UNLESS(datum.is_array());
            auto& reader = Readers_[columnIndex];
            return reader->GetItem(*datum.array(), blockIndex);
        }

        arrow::Datum GetScalarColumn(ui32 columnIndex) const {
            YQL_ENSURE(columnIndex < CurrentRow_.size());
            YQL_ENSURE(CurrentRow_[columnIndex].is_scalar());
            return CurrentRow_[columnIndex];
        }

    private:
        IDqInput::TPtr Input_;
        TUnboxedValueBatch FetchedValues_;

        TVector<arrow::Datum> CurrentRow_;
        ui64 CurrBlockIndex_ = 0;
        ui64 BlockLen_ = 0;
        TVector<std::unique_ptr<IBlockReader>> Readers_;
        const TDqInputMergeBlockStreamValue* const Parent_;
        bool IsFinished_ = false;
    };

    class TDqInputBatchIterator {
    public:
        explicit TDqInputBatchIterator(ui64 inputIndex, TDqInputBatch* data, ui64 blockIndex)
            : InputIndex_(inputIndex)
            , Data_(data)
            , BlockIndex_(blockIndex)
        {
            Y_DEBUG_ABORT_UNLESS(Data_);
        }

        bool operator<(const TDqInputBatchIterator& other) const {
            Y_DEBUG_ABORT_UNLESS(&Data_->Parent() == &other.Data_->Parent());
            const auto& comparators = Data_->Parent().Comparators_;
            ui32 comporatorIndex = 0;
            for (auto& sortCol : Data_->Parent().SortCols_) {
                ui32 idx = sortCol.Index;

                TBlockItem myValue = GetItem(idx);
                TBlockItem otherValue = other.GetItem(idx);

                i64 compare = comparators[comporatorIndex++]->Compare(myValue, otherValue);
                if (!sortCol.Ascending) {
                    compare = -compare;
                }

                if (compare) {
                    return compare > 0;
                }
            }
            // resolve equal elements: when InputIndex()es are equal, we must first process element with smaller BlockIndex()
            // Note: operator> here since this comparator is used together with max-heap
            return std::tuple(InputIndex(), BlockIndex()) > std::tuple(other.InputIndex(), other.BlockIndex());
        }

        TBlockItem GetItem(ui32 columnIndex) const {
            return Data_->GetColumnItem(columnIndex, BlockIndex_);
        }

        TDqInputBatch& Input() const {
            return *Data_;
        }

        ui64 BlockIndex() const {
            return BlockIndex_;
        }

        size_t InputIndex() const {
            return InputIndex_;
        }

    private:
        size_t InputIndex_ = 0;
        TDqInputBatch* Data_;
        ui64 BlockIndex_ = 0;
    };

    NUdf::EFetchStatus Fetch(NKikimr::NUdf::TUnboxedValue& result) final {
        Y_UNUSED(result);
        YQL_ENSURE(false, "Using Fetch() on wide block input");
    }

    NUdf::EFetchStatus WideFetch(NKikimr::NUdf::TUnboxedValue* result, ui32 width) final {
        YQL_ENSURE(width == ItemTypes_.size() + 1);
        if (IsFinished_) {
            return NUdf::EFetchStatus::Finish;
        }

        std::vector<arrow::Datum> chunk;
        ui64 blockLen = 0;
        while (!Output_ || !Output_->Next(chunk, blockLen)) {
            auto status = DoMerge();
            if (status != NUdf::EFetchStatus::Ok) {
                IsFinished_ = status == NUdf::EFetchStatus::Finish;
                return status;
            }
            YQL_ENSURE(Output_);
        }

        YQL_ENSURE(width == chunk.size() + 1);
        for (ui32 i = 0; i < chunk.size(); ++i) {
            result[i] = Factory_.CreateArrowBlock(std::move(chunk[i]));
        }

        YQL_ENSURE(blockLen > 0);
        result[chunk.size()] = Factory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(blockLen)));
        if (Stats_) {
            Stats_.Add(result, width);
        }
        return NUdf::EFetchStatus::Ok;
    }

    NUdf::EFetchStatus FetchInput(size_t index) {
        YQL_ENSURE(InputData_[index].IsEmpty());
        auto status = InputData_[index].FetchNext();
        if (status == NUdf::EFetchStatus::Ok) {
            if (!FirstSeenInputIndex_.Defined()) {
                FirstSeenInputIndex_ = index;
            }
            ui64 idx = InputData_[index].CurrentIndex();
            ui64 len = InputData_[index].BlockLen();
            while (idx < len) {
                InputRows_.emplace_back(index, &InputData_[index], idx++);
            }
        }
        return status;
    }

    NUdf::EFetchStatus DoMerge() {
        if (!Initialized_) {
            for (size_t i = StartInputIndex_; i < InputData_.size(); ++i) {
                auto status = FetchInput(i);
                if (status == NUdf::EFetchStatus::Yield) {
                    return status;
                }
                ++StartInputIndex_;
            }
            std::make_heap(InputRows_.begin(), InputRows_.end());
            Initialized_ = true;
        }

        if (StartInputIndex_ < InputData_.size()) {
            auto status = FetchInput(StartInputIndex_);
            if (status == NUdf::EFetchStatus::Yield) {
                return status;
            }
            if (status == NUdf::EFetchStatus::Ok) {
                std::make_heap(InputRows_.begin(), InputRows_.end());
            }
            StartInputIndex_ = InputData_.size();
        }

        while (OutputBlockLen_ < MaxOutputBlockLen_ && !InputRows_.empty()) {
            std::pop_heap(InputRows_.begin(), InputRows_.end());
            auto& smallest = InputRows_.back();

            TDqInputBatch& input = smallest.Input();
            const auto inputIndex = smallest.InputIndex();
            YQL_ENSURE(!input.IsEmpty());
            YQL_ENSURE(smallest.BlockIndex() == input.CurrentIndex(), "Sort order violation on input #" << inputIndex);

            for (size_t i = 0; i < Builders_.size(); ++i) {
                if (Builders_[i]) {
                    Builders_[i]->Add(smallest.GetItem(i));
                }
            }
            OutputBlockLen_++;
            input.NextRow();
            InputRows_.pop_back();
            if (input.IsEmpty()) {
                auto status = input.FetchNext();
                if (status == NUdf::EFetchStatus::Yield) {
                    StartInputIndex_ = inputIndex;
                    return status;
                }
                if (status == NUdf::EFetchStatus::Ok) {
                    std::make_heap(InputRows_.begin(), InputRows_.end());
                }
            }            
        }

        if (!OutputBlockLen_) {
            return NUdf::EFetchStatus::Finish;
        }

        std::vector<arrow::Datum> output;
        Y_DEBUG_ABORT_UNLESS(FirstSeenInputIndex_.Defined());
        for (size_t i = 0; i < Builders_.size(); ++i) {
            if (Builders_[i]) {
                output.emplace_back(Builders_[i]->Build(false));
            } else {
                output.emplace_back(InputData_[*FirstSeenInputIndex_].GetScalarColumn(i));
            }
        }

        Output_ = std::make_unique<TArgsDechunker>(std::move(output));
        OutputBlockLen_ = 0;
        return NUdf::EFetchStatus::Ok;
    }
private:
    TVector<TSortColumnInfo> SortCols_;
    const TVector<NKikimr::NMiniKQL::TType*> ItemTypes_;
    const ui64 MaxOutputBlockLen_;
    TVector<IBlockItemComparator::TPtr> Comparators_;

    TVector<TDqInputBatch> InputData_;
    TMaybe<size_t> FirstSeenInputIndex_;
    bool Initialized_ = false;
    size_t StartInputIndex_ = 0;
    TVector<TDqInputBatchIterator> InputRows_;


    TVector<std::unique_ptr<IArrayBuilder>> Builders_;
    ui64 OutputBlockLen_ = 0;

    const NKikimr::NMiniKQL::THolderFactory& Factory_;
    TDqMeteringStats::TInputStatsMeter Stats_;
    
    std::unique_ptr<TArgsDechunker> Output_;
    bool IsFinished_ = false;
};

void ValidateInputTypes(const NKikimr::NMiniKQL::TType* type, const TVector<IDqInput::TPtr>& inputs) {
    YQL_ENSURE(type);
    for (size_t i = 0; i < inputs.size(); ++i) {
        auto inputType = inputs[i]->GetInputType();
        YQL_ENSURE(inputType);
        YQL_ENSURE(type->IsSameType(*inputType), "Unexpected type for input #" << i << ": expected " << *type << ", got " << *inputType);
    }
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
    if (InputType) {
        YQL_ENSURE(InputType->IsMulti());
        auto multiType = static_cast<const TMultiType*>(InputType);
        YQL_ENSURE(width == multiType->GetElementsCount());
        const bool isBlock = AnyOf(multiType->GetElements(), [](auto itemType) {  return itemType->IsBlock(); });
        if (isBlock) {
            Stats->RowsConsumed += TArrowBlock::From(row[width - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
        } else {
            Stats->RowsConsumed += 1;
        }
        
        NYql::NDq::TDqDataSerializer::TEstimateSizeSettings settings;
        settings.DiscardUnsupportedTypes = true;
        settings.WithHeaders = false;

        ui64 size = 0;
        for (ui32 i = 0; (isBlock ? (i + 1) : i) < multiType->GetElementsCount(); ++i) {
            size += TDqDataSerializer::EstimateSize(row[i], multiType->GetElementType(i), nullptr, settings);
        }

        Stats->BytesConsumed += Max<ui64>(size, 8 /* billing size for count(*) */);
    } else {
        Stats->RowsConsumed += 1;
    }
}

NUdf::TUnboxedValue CreateInputUnionValue(const NKikimr::NMiniKQL::TType* type, TVector<IDqInput::TPtr>&& inputs,
    const NMiniKQL::THolderFactory& factory, TDqMeteringStats::TInputStatsMeter stats)
{
    ValidateInputTypes(type, inputs);
    if (type->IsMulti()) {
        return factory.Create<TDqInputUnionStreamValue<true>>(type, std::move(inputs), stats);
    }
    return factory.Create<TDqInputUnionStreamValue<false>>(type, std::move(inputs), stats);
}

NKikimr::NUdf::TUnboxedValue CreateInputMergeValue(const NKikimr::NMiniKQL::TType* type, TVector<IDqInput::TPtr>&& inputs,
    TVector<TSortColumnInfo>&& sortCols, const NKikimr::NMiniKQL::THolderFactory& factory, TDqMeteringStats::TInputStatsMeter stats)
{
    ValidateInputTypes(type, inputs);
    YQL_ENSURE(!inputs.empty());
    if (type->IsMulti()) {
        if (AnyOf(sortCols, [](const auto& sortCol) { return sortCol.IsBlockOrScalar(); })) {
            // we can ignore scalar columns, since all they have exactly the same value in all inputs
            EraseIf(sortCols, [](const auto& sortCol) { return *sortCol.IsScalar; });
            if (sortCols.empty()) {
                return factory.Create<TDqInputUnionStreamValue<true>>(type, std::move(inputs), stats);
            }
            return factory.Create<TDqInputMergeBlockStreamValue>(type, std::move(inputs), std::move(sortCols), factory, stats);
        }
        return factory.Create<TDqInputMergeStreamValue<true>>(type, std::move(inputs), std::move(sortCols), stats);
    }
    return factory.Create<TDqInputMergeStreamValue<false>>(type, std::move(inputs), std::move(sortCols), stats);
}

} // namespace NYql::NDq
