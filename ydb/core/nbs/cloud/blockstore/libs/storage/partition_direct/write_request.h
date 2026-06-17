#pragma once

#include "public.h"

#include "request_executor.h"
#include "write_request_bundle.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/wilson/wilson_span.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TWriteRequestExecutor
    : public IRequestExecutor
    , public std::enable_shared_from_this<TWriteRequestExecutor>
{
public:
    TWriteRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TLogTitle& logTitle,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        std::shared_ptr<TWriteRequestBundle> bundle);

    ~TWriteRequestExecutor() override;

    // Implementation of IRequestExecutor
    void Run() override;
    TString Print() override;

private:
    void SendIndirectWriteRequest();
    void OnIndirectWriteResponse(
        const TDBGWriteBlocksToManyPBuffersResponse& response);
    void SendAdditionalDirectWrites();
    void SendDirectWriteRequestsToDesired(size_t count);
    void SendDirectWriteRequestsToHandoffs(size_t count);
    void SendDirectWriteRequest(THostIndex host);
    void OnDirectWriteResponse(
        THostIndex host,
        const TDBGWriteBlocksResponse& response,
        std::shared_ptr<NWilson::TSpan> span);

    void ReplyOrNotifyBelated(
        NProto::TError error,
        THostMask completedOnCurrentResponse);
    void Reply(NProto::TError error);
    void NotifyBelated(THostMask completedOnCurrentResponse);

    void ScheduleHedging();
    void ScheduleRequestTimeout();
    void OnHedgingTimeout();
    void OnRequestTimeout();

    [[nodiscard]] bool ShouldReplyOk() const;
    [[nodiscard]] bool IsQuorumReachable() const;
    [[nodiscard]] size_t GetQuorumDeficit() const;
    [[nodiscard]] THostMask GetRunningDirectWrites() const;

    TString ExtendedDebugState() const;

    NActors::TActorSystem* ActorSystem;
    const EWriteMode WriteMode;
    const TChildLogTitle LogTitle;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TWriteRequestBundlePtr Bundle;
    const TDuration HedgingDelay;
    const TDuration RequestTimeout;
    const TDuration IndirectWriteReplyTimeout;

    THostMask IndirectCoordinator;
    THostMask RequestedIndirectWrites;
    THostMask RequestedDirectWrites;
    THostMask CompletedWrites;
    THostMask FailedWrites;
    bool IsReplied = false;
};

using TWriteRequestExecutorPtr = std::shared_ptr<TWriteRequestExecutor>;

////////////////////////////////////////////////////////////////////////////////

TWriteRequestExecutorPtr CreateWriteRequestExecutor(
    NActors::TActorSystem* const actorSystem,
    const TLogTitle& logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    std::shared_ptr<TWriteRequestBundle> bundle);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
