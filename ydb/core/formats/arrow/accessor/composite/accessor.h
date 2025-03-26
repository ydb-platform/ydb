#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NArrow::NAccessor {

class ICompositeChunkedArray: public NArrow::NAccessor::IChunkedArray {
private:
    using TBase = NArrow::NAccessor::IChunkedArray;
    virtual std::shared_ptr<IChunkedArray> DoISlice(const ui32 offset, const ui32 count) const override final;
    virtual std::shared_ptr<IChunkedArray> DoApplyFilter(const TColumnFilter& filter) const override;

public:
    using TBase::TBase;
};

class TCompositeChunkedArray: public ICompositeChunkedArray {
private:
    using TBase = ICompositeChunkedArray;

private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<IChunkedArray>>, Chunks);

    virtual void DoVisitValues(const TValuesSimpleVisitor& visitor) const override {
        for (auto&& i : Chunks) {
            i->VisitValues(visitor);
        }
    }

protected:
    virtual ui32 DoGetNullsCount() const override {
        AFL_VERIFY(false);
        return 0;
    }
    virtual ui32 DoGetValueRawBytes() const override {
        AFL_VERIFY(false);
        return 0;
    }

    virtual TLocalChunkedArrayAddress DoGetLocalChunkedArray(
        const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 /*index*/) const override {
        AFL_VERIFY(false)("problem", "cannot use method");
        return nullptr;
    }
    virtual std::optional<ui64> DoGetRawSize() const override {
        return {};
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override {
        AFL_VERIFY(false);
        return nullptr;
    }
    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override;

public:
    TCompositeChunkedArray(std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& chunks, const ui32 recordsCount,
        const std::shared_ptr<arrow::DataType>& type)
        : TBase(recordsCount, NArrow::NAccessor::IChunkedArray::EType::CompositeChunkedArray, type)
        , Chunks(std::move(chunks)) {
    }

    virtual std::optional<bool> DoCheckOneValueAccessor(std::shared_ptr<arrow::Scalar>& value) const override;

    virtual bool HasSubColumnData(const TString& subColumnName) const override {
        for (auto&& i : Chunks) {
            if (!i->HasSubColumnData(subColumnName)) {
                return false;
            }
        }
        return true;
    }

    virtual bool HasWholeDataVolume() const override {
        for (auto&& i : Chunks) {
            if (!i->HasWholeDataVolume()) {
                return false;
            }
        }
        return true;
    }

    class TIterator: TNonCopyable {
    private:
        const std::shared_ptr<TCompositeChunkedArray> Owner;
        ui32 RecordIndex = 0;
        std::optional<TFullChunkedArrayAddress> CurrentChunk;

    public:
        TIterator(const std::shared_ptr<TCompositeChunkedArray>& owner)
            : Owner(owner) {
            if (Owner->GetRecordsCount()) {
                CurrentChunk = Owner->GetArray(CurrentChunk, RecordIndex, Owner);
            }
        }

        const std::shared_ptr<IChunkedArray>& GetArray() const {
            AFL_VERIFY(CurrentChunk);
            return CurrentChunk->GetArray();
        }

        bool IsValid() {
            return RecordIndex < Owner->GetRecordsCount();
        }

        bool Next() {
            AFL_VERIFY(IsValid());
            AFL_VERIFY(CurrentChunk);
            RecordIndex += CurrentChunk->GetArray()->GetRecordsCount();
            AFL_VERIFY(RecordIndex <= Owner->GetRecordsCount());
            if (IsValid()) {
                CurrentChunk = Owner->GetArray(CurrentChunk, RecordIndex, Owner);
                return true;
            } else {
                return false;
            }
        }
    };

    static TIterator BuildIterator(std::shared_ptr<TCompositeChunkedArray>& owner) {
        return TIterator(owner);
    }

    class TBuilder {
    private:
        ui32 RecordsCount = 0;
        std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> Chunks;
        const std::shared_ptr<arrow::DataType> Type;
        bool Finished = false;

    public:
        TBuilder(const std::shared_ptr<arrow::DataType>& type)
            : Type(type) {
            AFL_VERIFY(Type);
        }

        void AddChunk(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& arr) {
            AFL_VERIFY(!Finished);
            AFL_VERIFY(arr->GetDataType()->id() == Type->id())("incoming", arr->GetDataType()->ToString())("main", Type->ToString());
            Chunks.emplace_back(arr);
            RecordsCount += arr->GetRecordsCount();
        }

        std::shared_ptr<IChunkedArray> Finish() {
            AFL_VERIFY(!Finished);
            Finished = true;
            if (Chunks.size() == 1) {
                return Chunks.front();
            }
            return std::shared_ptr<TCompositeChunkedArray>(new TCompositeChunkedArray(std::move(Chunks), RecordsCount, Type));
        }
    };
};

}   // namespace NKikimr::NArrow::NAccessor
