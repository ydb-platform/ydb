#pragma once

#include "public.h"

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

#include <functional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TBaseWriteRequestExecutor
    : public std::enable_shared_from_this<TBaseWriteRequestExecutor>
{
public:
    using TReplyCallback = std::function<void(TWriteRequestResponse)>;
    using TNotifyBelatedCallback = std::function<void(THostMask, ui64)>;

    TBaseWriteRequestExecutor(
        NActors::TActorSystem* actorSystem,
        TChildLogTitle logTitle,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TBlockRange64 vChunkRange,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        ui64 lsn,
        NWilson::TTraceId traceId);

    virtual ~TBaseWriteRequestExecutor();

    void SetReplyCallback(TReplyCallback callback);
    void SetNotifyBelatedCallback(TNotifyBelatedCallback callback);

    [[nodiscard]] bool IsAlreadyReplied() const;

    virtual void Run() = 0;

protected:
    void ReplyOrNotifyBelated(
        NProto::TError error,
        THostMask completedOnCurrentResponse);
    void Reply(NProto::TError error);
    void NotifyBelated(THostMask completedOnCurrentResponse);

    void SendWriteRequest(THostIndex host);

    virtual void OnWriteResponse(
        THostIndex host,
        const TDBGWriteBlocksResponse& response,
        std::shared_ptr<NWilson::TSpan> span);

    void ScheduleRequestTimeoutCallback();
    void RequestTimeoutCallback();
    [[nodiscard]] bool ShouldReplyOk() const;

    TVector<THostIndex> GetAvailableHandOffHosts() const;
    virtual TString ExtendedDebugState() const;

    virtual void ScheduleHedging() = 0;

    NActors::TActorSystem* ActorSystem;
    const TChildLogTitle LogTitle;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TBlockRange64 VChunkRange;
    const TCallContextPtr CallContext;
    const std::shared_ptr<TWriteBlocksLocalRequest> Request;
    const NWilson::TTraceId TraceId;
    const ui64 Lsn;
    const TDuration HedgingDelay;
    const TDuration RequestTimeout;

    THostMask RequestedWrites;
    THostMask CompletedWrites;

private:
    TReplyCallback ReplyCallback;
    TNotifyBelatedCallback NotifyBelatedCallback;
    bool IsReplied = false;
};

using TBaseWriteRequestExecutorPtr = std::shared_ptr<TBaseWriteRequestExecutor>;

////////////////////////////////////////////////////////////////////////////////

TBaseWriteRequestExecutorPtr CreateWriteRequestExecutor(
    NActors::TActorSystem* actorSystem,
    const TLogTitle& logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TBlockRange64 vChunkRange,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    ui64 lsn,
    NWilson::TTraceId traceId);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
