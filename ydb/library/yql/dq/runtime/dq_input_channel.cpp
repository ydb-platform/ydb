#include "dq_input_channel.h"
#include "dq_input_impl.h"

namespace NYql::NDq {

class TDqInputChannelImpl : public TDqInputImpl<TDqInputChannelImpl, IDqInputChannel> {
    using TBaseImpl = TDqInputImpl<TDqInputChannelImpl, IDqInputChannel>;

public:
    TDqInputChannelStats PushStats;
    TDqInputStats PopStats;

    TDqInputChannelImpl(ui64 channelId, ui32 srcStageId, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes, TCollectStatsLevel level,
        const NKikimr::NMiniKQL::TTypeEnvironment&, const NKikimr::NMiniKQL::THolderFactory&,
        NDqProto::EDataTransportVersion)
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

private:
    void Push(TDqSerializedBatch&&) override {
        Y_ABORT("Not implemented");
    }
};

class TDqInputChannel : public IDqInputChannel {

private:
    std::deque<TDqSerializedBatch> DataForDeserialize;
    ui64 StoredSerializedBytes = 0;

    void PushImpl(TDqSerializedBatch&& data) {
        const i64 space = data.Size();
        const size_t rowCount = data.RowCount();
        auto inputType = Impl.GetInputType();
        NKikimr::NMiniKQL::TUnboxedValueBatch batch(inputType);
        if (Y_UNLIKELY(PushStats.CollectProfile())) {
            auto startTime = TInstant::Now();
            DataSerializer.Deserialize(std::move(data), inputType, batch);
            PushStats.DeserializationTime += (TInstant::Now() - startTime);
        } else {
            DataSerializer.Deserialize(std::move(data), inputType, batch);
        }

        YQL_ENSURE(batch.RowCount() == rowCount);
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
    TDqInputChannelStats PushStats;
    TDqInputStats PopStats;

    TDqInputChannel(ui64 channelId, ui32 srcStageId, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes, TCollectStatsLevel level,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv, const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        NDqProto::EDataTransportVersion transportVersion)
        : Impl(channelId, srcStageId, inputType, maxBufferBytes, level, typeEnv, holderFactory, transportVersion)
        , DataSerializer(typeEnv, holderFactory, transportVersion) {
    }

    ui64 GetChannelId() const override {
        return Impl.GetChannelId();
    }

    const TDqInputChannelStats& GetPushStats() const override {
        return Impl.GetPushStats();
    }

    const TDqInputStats& GetPopStats() const override {
        return Impl.GetPopStats();
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

    void Pause() override {
        DeserializeAllData();
        Impl.Pause();
    }

    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) override {
        if (Impl.Empty() && !Impl.IsPaused()) {
            DeserializeAllData();
        }
        return Impl.Pop(batch);
    }

    void Push(TDqSerializedBatch&& data) override {
        YQL_ENSURE(!Impl.IsFinished(), "input channel " << PushStats.ChannelId << " already finished");
        if (Y_UNLIKELY(data.Proto.GetRows() == 0)) {
            return;
        }
        StoredSerializedBytes += data.Size();
        DataForDeserialize.emplace_back(std::move(data));
    }

    NKikimr::NMiniKQL::TType* GetInputType() const override {
        return Impl.GetInputType();
    }

    void Resume() override {
        Impl.Resume();
    }

    bool IsPaused() const override {
        return Impl.IsPaused();
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
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, NDqProto::EDataTransportVersion transportVersion)
{
    return new TDqInputChannel(channelId, srcStageId, inputType, maxBufferBytes, level, typeEnv, holderFactory,
        transportVersion);
}

} // namespace NYql::NDq
