#pragma once
#include "dq_input.h"
#include "dq_transport.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql::NDq {

struct TDqInputChannelStats : TDqInputStats {
    ui64 ChannelId = 0;

    // profile stats
    TDuration DeserializationTime;

    explicit TDqInputChannelStats(ui64 channelId)
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
        this->DeserializationTime = TDuration::MicroSeconds(f.GetDeserializationTimeUs());
    }
};

class IDqInputChannel : public IDqInput {
public:
    using TPtr = TIntrusivePtr<IDqInputChannel>;

    virtual ui64 GetChannelId() const = 0;

    virtual void Push(TDqSerializedBatch&& data) = 0;

    virtual void Finish() = 0;

    virtual const TDqInputChannelStats* GetStats() const = 0;
};

IDqInputChannel::TPtr CreateDqInputChannel(ui64 channelId, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes,
    bool collectProfileStats, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, NDqProto::EDataTransportVersion transportVersion);

} // namespace NYql::NDq
