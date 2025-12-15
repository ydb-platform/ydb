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

    void Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) override { // noop
        Y_UNUSED(outputActorId);
        Y_UNUSED(inputActorId);
    }

private:
    void Push(TDqSerializedBatch&&) override {
        Y_ABORT("Not implemented");
    }

    void Push(TInstant) override {
        Y_ABORT("Not implemented");
    }
};

class TDqInputChannel : public IDqInputChannel {

private:
    std::deque<std::variant<TDqSerializedBatch, TInstant>> DataForDeserialize;
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
            auto& data = DataForDeserialize.front();
            std::visit(TOverloaded {
                [this](TDqSerializedBatch data) {
                    PushImpl(std::move(data));
                },
                [this](TInstant watermark) {
                    Impl.PushWatermark(watermark);
                },
            }, data);
            DataForDeserialize.pop_front();
        }
        StoredSerializedBytes = 0;
    }

public:
    TDqInputChannel(const TDqChannelSettings& settings, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv)
        : Impl(settings.ChannelId, settings.SrcStageId, settings.RowType, settings.MaxStoredBytes, settings.Level)
        , DataSerializer(typeEnv, *settings.HolderFactory, settings.TransportVersion, settings.PackerVersion) {
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

    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>& watermark) override {
        if (Impl.Empty() && !Impl.IsPaused()) {
            DeserializeAllData();
        }
        return Impl.Pop(batch, watermark);
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

    void Push(TInstant watermark) override {
        YQL_ENSURE(!Impl.IsFinished(), "input channel " << Impl.PushStats.ChannelId << " already finished");

        DataForDeserialize.emplace_back(watermark);
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

    void Bind(NActors::TActorId outputActorId, NActors::TActorId inputActorId) override { // noop
        Y_UNUSED(outputActorId);
        Y_UNUSED(inputActorId);
    }

private:
    TDqInputChannelImpl Impl;
    TDqDataSerializer DataSerializer;
};

IDqInputChannel::TPtr CreateDqInputChannel(const TDqChannelSettings& settings, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv)
{
    return new TDqInputChannel(settings, typeEnv);
}

} // namespace NYql::NDq
