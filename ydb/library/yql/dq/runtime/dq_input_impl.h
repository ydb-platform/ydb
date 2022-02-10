#pragma once

namespace NYql::NDq {

template <class TDerived, class IInputInterface>
class TDqInputImpl : public IInputInterface {
public:
    TDqInputImpl(NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes)
        : InputType(inputType)
        , MaxBufferBytes(maxBufferBytes)
    {
    }

    i64 GetFreeSpace() const override {
        return (i64) MaxBufferBytes - StoredBytes;
    }

    ui64 GetStoredBytes() const override {
        return StoredBytes;
    }

    [[nodiscard]]
    bool Empty() const override {
        return Batches.empty() || (IsPaused() && GetBatchesBeforePause() == 0);
    }

    void AddBatch(NKikimr::NMiniKQL::TUnboxedValueVector&& batch, i64 space) {
        StoredBytes += space;
        StoredRows += batch.size();

        auto& stats = MutableBasicStats(); 
        stats.Chunks++; 
        stats.Bytes += space; 
        stats.RowsIn += batch.size(); 
        if (!stats.FirstRowTs) { 
            stats.FirstRowTs = TInstant::Now(); 
        }

        if (auto* profile = MutableProfileStats()) { 
            profile->MaxMemoryUsage = std::max(profile->MaxMemoryUsage, StoredBytes); 
        } 
 
        Batches.emplace_back(std::move(batch));
    }

    [[nodiscard]]
    bool Pop(NKikimr::NMiniKQL::TUnboxedValueVector& batch) override {
        if (Empty()) {
            return false;
        }

        batch.clear();

        if (IsPaused()) {
            ui64 batchesCount = GetBatchesBeforePause();
            Y_VERIFY(batchesCount <= Batches.size());

            batch.reserve(StoredRowsBeforePause);

            while (batchesCount--) {
                auto& part = Batches.front();
                std::move(part.begin(), part.end(), std::back_inserter(batch));
                Batches.pop_front();
            }

            BatchesBeforePause = PauseMask;
            Y_VERIFY(GetBatchesBeforePause() == 0);
            StoredBytes -= StoredBytesBeforePause;
            StoredRows -= StoredRowsBeforePause;
            StoredBytesBeforePause = 0;
            StoredRowsBeforePause = 0;
        } else {
            batch.reserve(StoredRows);

            for (auto&& part : Batches) {
                std::move(part.begin(), part.end(), std::back_inserter(batch));
            }

            StoredBytes = 0;
            StoredRows = 0;
            Batches.clear();
        }

        MutableBasicStats().RowsOut += batch.size(); 
        return true;
    }

    void Finish() override {
        Finished = true; 
    }

    bool IsFinished() const override {
        return Finished && (!IsPaused() || Batches.empty());
    }

    NKikimr::NMiniKQL::TType* GetInputType() const override {
        return InputType;
    }

    auto& MutableBasicStats() { 
        return static_cast<TDerived*>(this)->BasicStats; 
    }

    auto* MutableProfileStats() { 
        return static_cast<TDerived*>(this)->ProfileStats; 
    } 
 
    void Pause() override {
        Y_VERIFY(!IsPaused());
        if (!Finished) {
            BatchesBeforePause = Batches.size() | PauseMask;
            StoredRowsBeforePause = StoredRows;
            StoredBytesBeforePause = StoredBytes;
        }
    }

    void Resume() override {
        StoredBytesBeforePause = StoredRowsBeforePause = BatchesBeforePause = 0;
        Y_VERIFY(!IsPaused());
    }

    bool IsPaused() const override {
        return BatchesBeforePause;
    }

protected:
    ui64 GetBatchesBeforePause() const {
        return BatchesBeforePause & ~PauseMask;
    }

protected:
    NKikimr::NMiniKQL::TType* const InputType = nullptr;
    const ui64 MaxBufferBytes = 0;
    TList<NKikimr::NMiniKQL::TUnboxedValueVector, NKikimr::NMiniKQL::TMKQLAllocator<NUdf::TUnboxedValue>> Batches;
    ui64 StoredBytes = 0;
    ui64 StoredRows = 0;
    bool Finished = false;
    ui64 BatchesBeforePause = 0;
    ui64 StoredBytesBeforePause = 0;
    ui64 StoredRowsBeforePause = 0;
    static constexpr ui64 PauseMask = 1llu << 63llu;
};

} // namespace NYql::NDq
