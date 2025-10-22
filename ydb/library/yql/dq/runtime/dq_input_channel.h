#pragma once
#include "dq_input.h"
#include "dq_transport.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NDq {

struct TDqInputChannelStats : TDqInputStats {
    ui64 ChannelId = 0;
    ui32 SrcStageId = 0;
    ui64 RowsInMemory = 0;
    ui64 MaxMemoryUsage = 0;
    TDuration DeserializationTime;
};

class IDqInputChannel : public IDqInput {
public:
    using TPtr = TIntrusivePtr<IDqInputChannel>;

    virtual ui64 GetChannelId() const = 0;
    virtual const TDqInputChannelStats& GetPushStats() const = 0;

    virtual void Push(TDqSerializedBatch&& data) = 0;

    virtual void Finish() = 0;
};

IDqInputChannel::TPtr CreateDqInputChannel(ui64 channelId, ui32 srcStageId, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes,
    TCollectStatsLevel level, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, NDqProto::EDataTransportVersion transportVersion, NKikimr::NMiniKQL::EValuePackerVersion packerVersion);

} // namespace NYql::NDq
