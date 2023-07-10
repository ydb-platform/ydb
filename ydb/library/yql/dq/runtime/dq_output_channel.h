#pragma once
#include "dq_output.h"
#include "dq_channel_storage.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/common/dq_serialized_batch.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <util/generic/size_literals.h>


namespace NYql::NDq {

struct TDqOutputChannelStats : TDqOutputStats {
    ui64 ChannelId = 0;

    // profile stats
    TDuration SerializationTime;

    ui64 SpilledBytes = 0;
    ui64 SpilledRows = 0;
    ui64 SpilledBlobs = 0;

    explicit TDqOutputChannelStats(ui64 channelId)
        : ChannelId(channelId) {}

    template<typename T>
    void FromProto(const T& f)
    {
        this->ChannelId = f.GetChannelId();
        this->Chunks = f.GetChunks();
        this->Bytes = f.GetBytes();
        this->RowsIn = f.GetRowsIn();
        this->RowsOut = f.GetRowsOut();
        this->MaxMemoryUsage = f.GetMaxMemoryUsage();
        //s->StartTs = TInstant::MilliSeconds(f.GetStartTs());
        //s->FinishTs = TInstant::MilliSeconds(f.GetFinishTs());
    }
};

class IDqOutputChannel : public IDqOutput {
public:
    using TPtr = TIntrusivePtr<IDqOutputChannel>;

    virtual ui64 GetChannelId() const = 0;
    virtual ui64 GetValuesCount() const = 0;

    // <| consumer methods
    // can throw TDqChannelStorageException
    [[nodiscard]]
    virtual bool Pop(TDqSerializedBatch& data) = 0;
    // Pop watermark.
    [[nodiscard]]
    virtual bool Pop(NDqProto::TWatermark& watermark) = 0;
    // Pop chechpoint. Checkpoints may be taken from channel even after it is finished.
    [[nodiscard]]
    virtual bool Pop(NDqProto::TCheckpoint& checkpoint) = 0;
    // Only for data-queries
    // TODO: remove this method and create independent Data- and Stream-query implementations.
    //       Stream-query implementation should be without PopAll method.
    //       Data-query implementation should be one-shot for Pop (a-la PopAll) call and without ChannelStorage.
    // can throw TDqChannelStorageException
    [[nodiscard]]
    virtual bool PopAll(TDqSerializedBatch& data) = 0;
    // |>

    virtual ui64 Drop() = 0;

    virtual const TDqOutputChannelStats* GetStats() const = 0;
    virtual void Terminate() = 0;
};

struct TDqOutputChannelSettings {
    ui64 MaxStoredBytes = 8_MB;
    ui64 MaxChunkBytes = 2_MB;
    ui64 ChunkSizeLimit = 48_MB;
    NDqProto::EDataTransportVersion TransportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0;
    IDqChannelStorage::TPtr ChannelStorage;
    bool CollectProfileStats = false;
};

IDqOutputChannel::TPtr CreateDqOutputChannel(ui64 channelId, NKikimr::NMiniKQL::TType* outputType,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const TDqOutputChannelSettings& settings, const TLogFunc& logFunc = {});

} // namespace NYql::NDq
