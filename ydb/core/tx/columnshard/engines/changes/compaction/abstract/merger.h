#pragma once
#include <ydb/core/tx/columnshard/engines/changes/compaction/common/context.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/common/result.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NCompaction {

class TSourceReverseRemap {
private:
    std::optional<ui32> MinSourceIndex;
    std::optional<ui32> MaxSourceIndex;
    std::optional<ui32> MinResultIndex;
    std::optional<ui32> MaxResultIndex;
    YDB_READONLY_DEF(std::vector<i32>, Remapper);

public:
    TString DebugString() const {
        TStringBuilder sb;
        if (!IsEmpty()) {
            sb << "{src=[" << *MinSourceIndex << "," << *MaxSourceIndex << "]"
               << ";res=[" << *MinResultIndex << "," << *MaxResultIndex << "]"
               << ";size=" << Remapper.size() << "}";
        } else {
            AFL_VERIFY(!MaxSourceIndex);
            AFL_VERIFY(Remapper.empty());
            sb << "{EMPTY}";
        }
        return sb;
    }

    std::optional<i64> RemapSourceIndex(const ui32 index) const {
        AFL_VERIFY(MinSourceIndex);
        AFL_VERIFY(*MaxSourceIndex - *MinSourceIndex + 1 == Remapper.size());
        if (index < *MinSourceIndex) {
            return std::nullopt;
        }
        if (*MaxSourceIndex < index) {
            return std::nullopt;
        }
        AFL_VERIFY(index - *MinSourceIndex < Remapper.size());
        return Remapper[index - *MinSourceIndex];
    }
    bool IsEmpty() const {
        return Remapper.empty();
    }
    ui32 GetMinSourceIndex() const {
        AFL_VERIFY(MinSourceIndex);
        return *MinSourceIndex;
    }
    ui32 GetMaxSourceIndex() const {
        AFL_VERIFY(MaxSourceIndex);
        return *MaxSourceIndex;
    }
    ui32 GetMinResultIndex() const {
        AFL_VERIFY(MinResultIndex);
        return *MinResultIndex;
    }
    ui32 GetMaxResultIndex() const {
        AFL_VERIFY(MaxResultIndex);
        return *MaxResultIndex;
    }
    bool ContainsResult(const ui32 position) const {
        if (IsEmpty()) {
            return false;
        }
        return *MinResultIndex <= position && position <= *MaxResultIndex;
    }
    bool ContainsSource(const ui32 position) const {
        if (IsEmpty()) {
            return false;
        }
        return *MinSourceIndex <= position && position <= *MaxSourceIndex;
    }
    void AddRemap(const ui32 sourceIndex, const ui32 resultIndex) {
        if (!MinSourceIndex) {
            MinSourceIndex = sourceIndex;
        } else {
            AFL_VERIFY(*MinSourceIndex < sourceIndex);
        }
        if (MaxSourceIndex) {
            AFL_VERIFY(*MaxSourceIndex < sourceIndex);
        }
        MaxSourceIndex = sourceIndex;

        if (!MinResultIndex) {
            MinResultIndex = resultIndex;
        } else {
            AFL_VERIFY(*MinResultIndex < resultIndex);
        }
        if (MaxResultIndex) {
            AFL_VERIFY(*MaxResultIndex < resultIndex);
        }
        MaxResultIndex = resultIndex;

        if (sourceIndex != Remapper.size() + *MinSourceIndex) {
            AFL_VERIFY(Remapper.size() + *MinSourceIndex < sourceIndex);
            while (Remapper.size() + *MinSourceIndex < sourceIndex) {
                Remapper.emplace_back(-1);
            }
        }
        Remapper.emplace_back(resultIndex);
    }
};

class TMergingChunkContext {
private:
    std::shared_ptr<arrow::UInt16Array> IdxArray;
    std::shared_ptr<arrow::UInt32Array> RecordIdxArray;
    std::optional<std::vector<TSourceReverseRemap>> ReverseIndexes;

public:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "{size=" << GetRecordsCount() << ";";
        if (ReverseIndexes) {
            sb << "[";
            for (auto&& i : *ReverseIndexes) {
                sb << i.DebugString() << ";";
            }
            sb << "]";
        }
        sb << "}";
        return sb;
    }

    void InitReverseIndexes(const ui32 sourcesCount) {
        if (!!ReverseIndexes) {
            return;
        }
        std::vector<TSourceReverseRemap> result;
        result.resize(sourcesCount);
        for (ui32 recordIdx = 0; recordIdx < GetRecordsCount(); ++recordIdx) {
            AFL_VERIFY(IdxArray->Value(recordIdx) < result.size());
            result[IdxArray->Value(recordIdx)].AddRemap(RecordIdxArray->Value(recordIdx), recordIdx);
        }
        ReverseIndexes = std::move(result);
    }

    ui32 GetRecordsCount() const {
        return IdxArray->length();
    }

    TMergingChunkContext(const std::shared_ptr<arrow::RecordBatch>& pkAndAddresses);

    class TView {
    private:
        TMergingChunkContext* Owner;
        YDB_READONLY(ui32, Offset, 0);
        YDB_READONLY(ui32, Size, 0);

        std::shared_ptr<arrow::UInt16Array> IdxArray;
        std::shared_ptr<arrow::UInt32Array> RecordIdxArray;

    public:
        TString DebugString() const {
            TStringBuilder sb;
            sb << "{" << "offset=" << Offset << ";size=" << Size << ";owner=" << Owner->DebugString() << "}";
            return sb;
        }
        ui32 GetRecordsCount() const {
            return Size;
        }

        void InitReverseIndexes(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input) const {
            Owner->InitReverseIndexes(input.size());
        }

        const TSourceReverseRemap& GetReverseIndexes(const ui32 idx) const {
            AFL_VERIFY(Owner->ReverseIndexes);
            AFL_VERIFY(idx < Owner->ReverseIndexes->size());
            return (*Owner->ReverseIndexes)[idx];
        }

        TView(TMergingChunkContext& owner, const ui32 offset, const ui32 size)
            : Owner(&owner)
            , Offset(offset)
            , Size(size) {
            AFL_VERIFY(offset + size <= Owner->GetRecordsCount());
            AFL_VERIFY(size);
            IdxArray = std::static_pointer_cast<arrow::UInt16Array>(Owner->IdxArray->Slice(offset, size));
            RecordIdxArray = std::static_pointer_cast<arrow::UInt32Array>(Owner->RecordIdxArray->Slice(offset, size));
        }

        const arrow::UInt16Array& GetIdxArray() const {
            return *IdxArray;
        }
        const arrow::UInt32Array& GetRecordIdxArray() const {
            return *RecordIdxArray;
        }
    };

    TView Slice(const ui32 offset, const ui32 size) {
        return TView(*this, offset, size);
    }
};

class TMergingContext {
private:
    std::vector<std::shared_ptr<NArrow::TGeneralContainer>> InputContainers;

public:
    TMergingContext(const std::vector<std::shared_ptr<NArrow::TGeneralContainer>>& inputContainers)
        : InputContainers(inputContainers) {
    }
};

class TChunkMergeContext {
private:
    const NColumnShard::TIndexationCounters& Counters;
    const TMergingChunkContext::TView Remapper;

public:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "{remapper=" << Remapper.DebugString() << "}";
        return sb;
    }

    const TMergingChunkContext::TView& GetRemapper() const {
        return Remapper;
    }

    const NColumnShard::TIndexationCounters& GetCounters() const {
        return Counters;
    }

    TChunkMergeContext(const NColumnShard::TIndexationCounters& counters, TMergingChunkContext::TView&& remapper)
        : Counters(counters)
        , Remapper(std::move(remapper)) {
    }
};

class IColumnMerger {
public:
    using TFactory = NObjectFactory::TParametrizedObjectFactory<IColumnMerger, TString, const TColumnMergeContext&>;

private:
    bool Started = false;

    virtual TColumnPortionResult DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) = 0;
    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) = 0;

protected:
    const TColumnMergeContext& Context;

public:
    template <class TArrayImpl>
    class TBaseIterator {
    private:
        std::shared_ptr<NArrow::NAccessor::IChunkedArray> Input;
        std::shared_ptr<TArrayImpl> CurrentArray;
        std::optional<NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress> CurrentChunk;
        std::shared_ptr<TColumnLoader> Loader;
        YDB_READONLY(ui32, LocalPosition, 0);
        YDB_READONLY(ui32, GlobalPosition, 0);

        bool InitArray(const ui32 globalPosition) {
            if (Input) {
                if (globalPosition == Input->GetRecordsCount()) {
                    GlobalPosition = globalPosition;
                    return false;
                }
                AFL_VERIFY(globalPosition < Input->GetRecordsCount())("pos", globalPosition)("size", Input->GetRecordsCount());
                CurrentChunk = Input->GetArray(CurrentChunk, globalPosition, Input);
                if (CurrentChunk->GetArray()->GetType() == TArrayImpl::GetTypeStatic()) {
                    CurrentArray = std::static_pointer_cast<TArrayImpl>(CurrentChunk->GetArray());
                } else {
                    CurrentArray = std::static_pointer_cast<TArrayImpl>(Loader->GetAccessorConstructor()
                            ->Construct(CurrentChunk->GetArray(), Loader->BuildAccessorContext(CurrentChunk->GetArray()->GetRecordsCount()))
                            .DetachResult());
                }
                LocalPosition = CurrentChunk->GetAddress().GetLocalIndex(globalPosition);
                GlobalPosition = globalPosition;
                OnInitArray(CurrentArray);
                return true;
            } else {
                AFL_VERIFY(globalPosition == 0);
                GlobalPosition = globalPosition;
                return false;
            }
        }

        virtual void OnInitArray(const std::shared_ptr<TArrayImpl>& /*arr*/) {
        
        }

    public:
        virtual ~TBaseIterator() = default;

        ui32 GetGlobalPosition(const ui32 localPosition) const {
            AFL_VERIFY(CurrentChunk);
            return CurrentChunk->GetAddress().GetGlobalIndex(localPosition);
        }

        bool IsEmpty() const {
            return !Input;
        }

        [[nodiscard]] bool MoveFurther(const ui32 delta) {
            return MoveToPosition(GlobalPosition + delta);
        }

        bool IsValid() const {
            return Input && GlobalPosition < Input->GetRecordsCount();
        }

        void Reset() {
            InitArray(0);
        }

        TBaseIterator(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& input, const std::shared_ptr<TColumnLoader>& loader)
            : Input(input)
            , Loader(loader) {
            AFL_VERIFY(Loader);
            InitArray(0);
        }

        const TArrayImpl& GetCurrentDataChunk() const {
            AFL_VERIFY(CurrentArray);
            return *CurrentArray;
        }

        [[nodiscard]] bool MoveToPosition(const ui32 globalPosition) {
            if (GlobalPosition == globalPosition) {
                return GlobalPosition < Input->GetRecordsCount();
            }
            AFL_VERIFY(Input);
            AFL_VERIFY(GlobalPosition < globalPosition)("old", GlobalPosition)("new", globalPosition)("count", Input->GetRecordsCount());
            if (CurrentChunk->GetAddress().Contains(globalPosition)) {
                LocalPosition = CurrentChunk->GetAddress().GetLocalIndex(globalPosition);
                GlobalPosition = globalPosition;
                return true;
            } else {
                return InitArray(globalPosition);
            }
        }
    };

    template <class TArrayImpl, class TConstructorImpl>
    class TPortionColumnChunkWriter: public TColumnPortionResult {
    private:
        using TBase = TColumnPortionResult;
        const TConstructorImpl Constructor;

    public:
        TPortionColumnChunkWriter(const TConstructorImpl& constructor, const ui32 columnId)
            : TBase(columnId)
            , Constructor(constructor) {
        }

        void AddChunk(const std::shared_ptr<TArrayImpl>& cArray, const TColumnMergeContext& cmContext) {
            AFL_VERIFY(cArray);
            AFL_VERIFY(cArray->GetRecordsCount());
            auto accContext = cmContext.GetLoader()->BuildAccessorContext(cArray->GetRecordsCount());
            Chunks.emplace_back(std::make_shared<NChunks::TChunkPreparation>(Constructor.SerializeToString(cArray, accContext), cArray,
                TChunkAddress(cmContext.GetColumnId(), Chunks.size()),
                cmContext.GetIndexInfo().GetColumnFeaturesVerified(cmContext.GetColumnId())));
        }
    };

    static inline const TString PortionIdFieldName = "$$__portion_id";
    static inline const TString PortionRecordIndexFieldName = "$$__portion_record_idx";
    static inline const std::shared_ptr<arrow::Field> PortionIdField =
        std::make_shared<arrow::Field>(PortionIdFieldName, std::make_shared<arrow::UInt16Type>());
    static inline const std::shared_ptr<arrow::Field> PortionRecordIndexField =
        std::make_shared<arrow::Field>(PortionRecordIndexFieldName, std::make_shared<arrow::UInt32Type>());

    IColumnMerger(const TColumnMergeContext& context)
        : Context(context) {
    }
    virtual ~IColumnMerger() = default;

    void Start(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext);

    TColumnPortionResult Execute(const TChunkMergeContext& context, TMergingContext& mergeContext) {
        auto result = DoExecute(context, mergeContext);
        AFL_VERIFY(result.GetRecordsCount() == context.GetRemapper().GetSize())("result", result.GetRecordsCount())(
                                                 "remapper", context.GetRemapper().GetSize());
        return result;
    }
};

}   // namespace NKikimr::NOlap::NCompaction
