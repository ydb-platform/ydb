#include "dq_input_channel.h"
#include "dq_input_impl.h"

namespace NYql::NDq {

class TDqInputChannel : public TDqInputImpl<TDqInputChannel, IDqInputChannel> {
    using TBaseImpl = TDqInputImpl<TDqInputChannel, IDqInputChannel>;
    friend TBaseImpl;
private:
    std::deque<NDqProto::TData> DataForDeserialize;
    ui64 StoredSerializedBytes = 0;

    void PushImpl(NDqProto::TData&& data) {
        const i64 space = data.GetRaw().size();

        NKikimr::NMiniKQL::TUnboxedValueBatch batch(InputType);
        if (Y_UNLIKELY(ProfileStats)) {
            auto startTime = TInstant::Now();
            DataSerializer.Deserialize(data, InputType, batch);
            ProfileStats->DeserializationTime += (TInstant::Now() - startTime);
        } else {
            DataSerializer.Deserialize(data, InputType, batch);
        }

        YQL_ENSURE(batch.RowCount() == data.GetRows());
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
    TDqInputChannel(ui64 channelId, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes, bool collectProfileStats,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv, const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        NDqProto::EDataTransportVersion transportVersion)
        : TBaseImpl(inputType, maxBufferBytes)
        , ChannelId(channelId)
        , BasicStats(ChannelId)
        , ProfileStats(collectProfileStats ? &BasicStats : nullptr)
        , DataSerializer(typeEnv, holderFactory, transportVersion)
    {}

    ui64 GetChannelId() const override {
        return ChannelId;
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

    void Push(NDqProto::TData&& data) override {
        YQL_ENSURE(!Finished, "input channel " << ChannelId << " already finished");
        if (Y_UNLIKELY(data.GetRows() == 0)) {
            return;
        }
        StoredSerializedBytes += data.GetRaw().size();
        DataForDeserialize.emplace_back(std::move(data));
    }

    const TDqInputChannelStats* GetStats() const override {
        return &BasicStats;
    }

private:
    const ui64 ChannelId;
    TDqInputChannelStats BasicStats;
    TDqInputChannelStats* ProfileStats = nullptr;
    TDqDataSerializer DataSerializer;
};

IDqInputChannel::TPtr CreateDqInputChannel(ui64 channelId, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes,
    bool collectProfileStats, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, NDqProto::EDataTransportVersion transportVersion)
{
    return new TDqInputChannel(channelId, inputType, maxBufferBytes, collectProfileStats, typeEnv, holderFactory,
        transportVersion);
}

} // namespace NYql::NDq
