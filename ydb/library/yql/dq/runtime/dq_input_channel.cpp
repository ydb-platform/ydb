#include "dq_input_channel.h"
#include "dq_input_impl.h"

namespace NYql::NDq {

class TDqInputChannelImpl : public TDqInputImpl<TDqInputChannelImpl, IDqInputChannel> {
    using TBaseImpl = TDqInputImpl<TDqInputChannelImpl, IDqInputChannel>;

public:
    using TBaseImpl::StoredBytes;

    TDqInputChannelStats PushStats;
    TDqInputStats PopStats;

    TDqInputChannelImpl(ui64 channelId, ui32 srcStageId, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes, TCollectStatsLevel level)
        : TBaseImpl(inputType, maxBufferBytes)
    {
        PopStats.Level = level;
        PushStats.Level = level;
        PushStats.ChannelId = channelId;
        PushStats.SrcStageId = srcStageId;
    }

    ui64 GetChannelId() const override {
        return PushStats.ChannelId;
    }

    const TDqInputChannelStats& GetPushStats() const override {
        return PushStats;
    }

    const TDqInputStats& GetPopStats() const override {
        return PopStats;
    }

    TString LogPrefix() const {
        return TStringBuilder() << "SrcStageId: " << PushStats.SrcStageId << " ChannelId: " << PushStats.ChannelId << ". ";
    }

    [[nodiscard]]
    bool Empty() const override {
        return Batches.empty() || IsPaused() && BeforeBarrier.Batches == 0;
    }

    void AddBatchCounts(ui64 space, ui64 rows) {
        auto& barrier = PendingBarriers.empty() ? BeforeBarrier : PendingBarriers.back();
        barrier.Batches ++;
        barrier.Bytes += space;
        barrier.Rows += rows;
    }

    std::tuple<ui64, ui64, ui64> PopReadyCounts() {
        if (!PendingBarriers.empty() && !IsPaused()) {
            // There were watermarks, but channel is not paused
            // Process data anyway and move watermarks behind
            auto lastBarrier = PendingBarriers.back().Barrier;
            for (const auto& barrier : PendingBarriers) {
                Y_ENSURE(!barrier.IsCheckpoint());
                BeforeBarrier += barrier;
            }
            PendingBarriers.clear();
            PendingBarriers.emplace_back(TBarrier { .Barrier = lastBarrier });
        }
        auto popBatches = BeforeBarrier.Batches;
        auto popBytes = BeforeBarrier.Bytes;
        auto popRows = BeforeBarrier.Rows;

        BeforeBarrier.Clear();
        return std::make_tuple(popBytes, popRows, popBatches);
    }

    bool IsPaused() const {
        return IsPausedByWatermark() || IsPausedByCheckpoint();
    }

private:
    void SkipWatermarksBeforeBarrier() {
        // Drop watermarks before current barrier
        while (!PendingBarriers.empty()) {
            auto& barrier = PendingBarriers.front();
            if (barrier.Barrier >= PauseBarrier) {
                break;
            }
            BeforeBarrier.Batches += barrier.Batches;
            BeforeBarrier.Rows += barrier.Rows;
            BeforeBarrier.Bytes += barrier.Bytes;
            PendingBarriers.pop_front();
        }
    }

public:
    void PauseByWatermark(TInstant watermark) override {
        Y_ENSURE(PauseBarrier <= watermark);
        PauseBarrier = watermark;
        if (IsPausedByCheckpoint()) {
            return;
        }
        if (IsFinished()) {
            return;
        }
        SkipWatermarksBeforeBarrier();
        Y_ENSURE(!PendingBarriers.empty());
        Y_ENSURE(PendingBarriers.front().Barrier >= watermark);
    }

    void PauseByCheckpoint() override {
        Y_ENSURE(!IsPausedByCheckpoint());
        if (PauseBarrier != TBarrier::NoBarrier) {
            Y_ENSURE(!PendingBarriers.empty());
            if (PendingBarriers.front().Barrier > PauseBarrier) {
                // (1.BeforeBarrier) (3.Watermark > PauseBarrier) (4.Some data and watermarks) | (5.Here will be checkpoint)
                // ->
                // (1.BeforeBarrier) (3.Fake watermark == PauseBarrier with data from 3 & 4 behind) (Checkpoint with empty data behind) (Max watermark from 3 & 4 with empty data behind)
                auto lastWatermark = PendingBarriers.back().Barrier;
                TBarrier fakeWatermark(PauseBarrier);
                for (auto& barrier: PendingBarriers) {
                    fakeWatermark += barrier;
                }
                PendingBarriers.clear();
                PendingBarriers.emplace_back(fakeWatermark);
                PendingBarriers.emplace_back(); // CheckpointBarrier
                PendingBarriers.emplace_back(TBarrier { .Barrier = lastWatermark });
            } else {
                Y_ENSURE(PendingBarriers.front().Barrier == PauseBarrier);
                // (1.BeforeBarrier) (3.Watermark == PauseBarrier) (4.Some data and watermarks) | (5.Here will be checkpoint)
                // ->
                // (1.BeforeBarrier) (3.Watermark == PauseBarriers with all data from 3 & 4 behind) (Checkpoint with empty data behind) (Max watermark from 4 with empty data behind)
                auto lastWatermark = PendingBarriers.size() > 1 ? PendingBarriers.back().Barrier : TBarrier::NoBarrier;
                for (auto& firstWatermark = PendingBarriers.front(); PendingBarriers.size() > 1; PendingBarriers.pop_back()) {
                    firstWatermark += PendingBarriers.back();
                }
                PendingBarriers.emplace_back(); // CheckpointBarrier
                if (lastWatermark != TBarrier::NoBarrier) {
                    PendingBarriers.emplace_back(TBarrier { .Barrier = lastWatermark });
                }
            }
        } else if (PendingBarriers.empty()) {
            PendingBarriers.emplace_front(); // CheckpointBarrier
        } else {
            // (1.BeforeBarrier) (4.Some data and watermarks) | (5.Here will be checkpoint)
            // ->
            // (1.BeforeBarrier + all data from 4) (5.Checkpoint with empty data behind) (Max watermark from 4 if any with empty data behind)
            auto lastWatermark = PendingBarriers.back().Barrier;
            for (auto& barrier: PendingBarriers) { // Move all collected data before checkpoint
                BeforeBarrier += barrier;
            }
            PendingBarriers.clear();
            PendingBarriers.emplace_back(); // CheckpointBarrier
            PendingBarriers.emplace_back(TBarrier { .Barrier = lastWatermark });
        }
    }

    void AddWatermark(TInstant watermark) override {
        if (!PendingBarriers.empty() && PendingBarriers.back().Batches == 0 && !PendingBarriers.back().IsCheckpoint()) {
            Y_ENSURE(PendingBarriers.back().Rows == 0);
            Y_ENSURE(PendingBarriers.back().Bytes == 0);
            PendingBarriers.back().Barrier = watermark;
        } else {
            PendingBarriers.emplace_back(TBarrier { .Barrier = watermark });
        }
    }

    bool IsPausedByWatermark() const override {
        return !IsPausedByCheckpoint() && PauseBarrier != TBarrier::NoBarrier;
    }

    bool IsPausedByCheckpoint() const override {
        return !PendingBarriers.empty() && PendingBarriers.front().IsCheckpoint();
    }

    void ResumeByWatermark(TInstant watermark) override {
        Y_ENSURE(Empty());
        Y_ENSURE(PauseBarrier == watermark);
        PauseBarrier = TBarrier::NoBarrier;
        if (IsFinished()) {
            return;
        }
        Y_ENSURE(!PendingBarriers.empty());
        Y_ENSURE(PendingBarriers.front().Barrier >= watermark);
        if (PendingBarriers.front().Barrier == watermark) {
            BeforeBarrier = PendingBarriers.front();
            PendingBarriers.pop_front();
        }
        Y_ENSURE(PendingBarriers.empty() || PendingBarriers.front().Barrier > watermark);
    }

    void ResumeByCheckpoint() override {
        Y_ENSURE(IsPausedByCheckpoint());
        Y_ENSURE(Empty());
        BeforeBarrier = PendingBarriers.front();
        PendingBarriers.pop_front();
        // There can be watermarks before current barrier exposed by checkpoint removal
        SkipWatermarksBeforeBarrier();
    }

private:
    void Push(TDqSerializedBatch&&) override {
        Y_ABORT("Not implemented");
    }

    struct TBarrier {
        static constexpr TInstant NoBarrier = TInstant::Zero();
        static constexpr TInstant CheckpointBarrier = TInstant::Max();
        TInstant Barrier = CheckpointBarrier;
        ui64 Batches = 0;
        ui64 Bytes = 0;
        ui64 Rows = 0;
        // watermark (!= TInstant::Max()) or checkpoint (TInstant::Max())
        bool IsCheckpoint() const {
            return Barrier == CheckpointBarrier;
        }
        TBarrier& operator+= (const TBarrier& other) {
            Batches += other.Batches;
            Bytes += other.Bytes;
            Rows += other.Rows;
            return *this;
        }
        void Clear() {
            Batches = 0;
            Bytes = 0;
            Rows = 0;
        }
    };
    std::deque<TBarrier> PendingBarriers; // barrier and counts after barrier
    TBarrier BeforeBarrier; // counts before barrier
    TInstant PauseBarrier; // 
};

class TDqInputChannel : public IDqInputChannel {

private:
    std::deque<TDqSerializedBatch> DataForDeserialize;
    ui64 StoredSerializedBytes = 0;

    void PushImpl(TDqSerializedBatch&& data) {
        const i64 space = data.Size();
        const size_t chunkCount = data.ChunkCount();
        auto inputType = Impl.GetInputType();
        NKikimr::NMiniKQL::TUnboxedValueBatch batch(inputType);
        if (Y_UNLIKELY(Impl.PushStats.CollectProfile())) {
            auto startTime = TInstant::Now();
            DataSerializer.Deserialize(std::move(data), inputType, batch);
            Impl.PushStats.DeserializationTime += (TInstant::Now() - startTime);
        } else {
            DataSerializer.Deserialize(std::move(data), inputType, batch);
        }

        // single batch row is chunk and may be Arrow block
        YQL_ENSURE(batch.RowCount() == chunkCount);
        Impl.AddBatch(std::move(batch), space);
    }

    void DeserializeAllData() {
        while (!DataForDeserialize.empty()) {
            PushImpl(std::move(DataForDeserialize.front()));
            DataForDeserialize.pop_front();
        }
        StoredSerializedBytes = 0;
    }

public:
    TDqInputChannel(ui64 channelId, ui32 srcStageId, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes, TCollectStatsLevel level,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv, const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion)
        : Impl(channelId, srcStageId, inputType, maxBufferBytes, level)
        , DataSerializer(typeEnv, holderFactory, transportVersion, packerVersion) {
    }

    ui64 GetChannelId() const override {
        return Impl.PushStats.ChannelId;
    }

    const TDqInputChannelStats& GetPushStats() const override {
        return Impl.PushStats;
    }

    const TDqInputStats& GetPopStats() const override {
        return Impl.PopStats;
    }

    i64 GetFreeSpace() const override {
        return Impl.GetFreeSpace() - i64(StoredSerializedBytes);
    }

    ui64 GetStoredBytes() const override {
        return Impl.GetStoredBytes() + StoredSerializedBytes;
    }

    bool IsFinished() const override {
        return DataForDeserialize.empty() && Impl.IsFinished();
    }

    bool Empty() const override {
        return (DataForDeserialize.empty() || Impl.IsPaused()) && Impl.Empty();
    }

    void PauseByCheckpoint() override {
        DeserializeAllData();
        Impl.PauseByCheckpoint();
    }

    void AddWatermark(TInstant watermark) override {
        DeserializeAllData();
        Impl.AddWatermark(watermark);
    }

    void PauseByWatermark(TInstant watermark) override {
        DeserializeAllData();
        Impl.PauseByWatermark(watermark);
    }

    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) override {
        if (Impl.Empty() && !Impl.IsPaused()) {
            DeserializeAllData();
        }
        return Impl.Pop(batch);
    }

    void Push(TDqSerializedBatch&& data) override {
        YQL_ENSURE(!Impl.IsFinished(), "input channel " << Impl.PushStats.ChannelId << " already finished");
        if (Y_UNLIKELY(data.Proto.GetChunks() == 0)) {
            return;
        }
        StoredSerializedBytes += data.Size();

        if (Impl.PushStats.CollectBasic()) {
            Impl.PushStats.Bytes += data.Size();
            Impl.PushStats.Rows += data.RowCount();
            Impl.PushStats.Chunks++;
            Impl.PushStats.Resume();
            if (Impl.PushStats.CollectFull()) {
                Impl.PushStats.MaxMemoryUsage = std::max(Impl.PushStats.MaxMemoryUsage, StoredSerializedBytes + Impl.StoredBytes);
            }
        }

        if (GetFreeSpace() < 0) {
            Impl.PopStats.TryPause();
        }

        DataForDeserialize.emplace_back(std::move(data));
    }

    NKikimr::NMiniKQL::TType* GetInputType() const override {
        return Impl.GetInputType();
    }

    void ResumeByCheckpoint() override {
        Impl.ResumeByCheckpoint();
    }

    bool IsPausedByCheckpoint() const override {
        return Impl.IsPausedByCheckpoint();
    }

    void ResumeByWatermark(TInstant watermark) override {
        Impl.ResumeByWatermark(watermark);
    }

    bool IsPausedByWatermark() const override {
        return Impl.IsPausedByWatermark();
    }

    void Finish() override {
        Impl.Finish();
    }

private:
    TDqInputChannelImpl Impl;
    TDqDataSerializer DataSerializer;
};

IDqInputChannel::TPtr CreateDqInputChannel(ui64 channelId, ui32 srcStageId, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes,
                                           TCollectStatsLevel level, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
                                           const NKikimr::NMiniKQL::THolderFactory& holderFactory, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion)
{
    return new TDqInputChannel(channelId, srcStageId, inputType, maxBufferBytes, level, typeEnv, holderFactory,
        transportVersion, packerVersion);
}

} // namespace NYql::NDq
