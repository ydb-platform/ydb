#include "dq_input_channel.h"
#include "dq_input_impl.h"

namespace NYql::NDq {

class TDqInputChannel : public TDqInputImpl<TDqInputChannel, IDqInputChannel> {
    using TBaseImpl = TDqInputImpl<TDqInputChannel, IDqInputChannel>;
    friend TBaseImpl;
private:
    std::deque<TDqSerializedBatch> DataForDeserialize;
    ui64 StoredSerializedBytes = 0;

    void PushImpl(TDqSerializedBatch&& data) {
        const i64 space = data.Size();
        const size_t rowCount = data.RowCount();

        NKikimr::NMiniKQL::TUnboxedValueBatch batch(InputType);
        if (Y_UNLIKELY(PushStats.CollectProfile())) {
            auto startTime = TInstant::Now();
            DataSerializer.Deserialize(std::move(data), InputType, batch);
            PushStats.DeserializationTime += (TInstant::Now() - startTime);
        } else {
            DataSerializer.Deserialize(std::move(data), InputType, batch);
        }

        YQL_ENSURE(batch.RowCount() == rowCount);
        AddBatch(std::move(batch), space);
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
        : TBaseImpl(inputType, maxBufferBytes)
        , DataSerializer(typeEnv, holderFactory, transportVersion)
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

    i64 GetFreeSpace() const override {
        return TBaseImpl::GetFreeSpace() - i64(StoredSerializedBytes);
    }

    ui64 GetStoredBytes() const override {
        return StoredBytes + StoredSerializedBytes;
    }

    bool IsFinished() const override {
        return DataForDeserialize.empty() && TBaseImpl::IsFinished();
    }

    [[nodiscard]]
    bool Empty() const override {
        return DataForDeserialize.empty() && TBaseImpl::Empty();
    }

    void Pause() override {
        DeserializeAllData();
        TBaseImpl::Pause();
    }

    [[nodiscard]]
    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) override {
        if (Batches.empty()) {
            DeserializeAllData();
        }
        return TBaseImpl::Pop(batch);
    }

    void Push(TDqSerializedBatch&& data) override {
        YQL_ENSURE(!Finished, "input channel " << PushStats.ChannelId << " already finished");
        if (Y_UNLIKELY(data.Proto.GetRows() == 0)) {
            return;
        }
        StoredSerializedBytes += data.Size();
        DataForDeserialize.emplace_back(std::move(data));
    }

private:
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
