#include "read_request_single_location.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TReadSingleLocationRequestExecutor::TReadSingleLocationRequestExecutor(
    NActors::TActorSystem const* actorSystem,
    const TLogTitle& logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TReadRangeHint readHint,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
    : ActorSystem(actorSystem)
    , LogTitle(logTitle.GetChildWithTags(
          GetCycleCount(),
          {{"t", "Read"},
           {"lsn", ToString(readHint.Lsn)},
           {"r", request->Headers.Range.Print()},
           {"vr", readHint.VChunkRange.Print()}}))
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
    , RequestTimeout(DirectBlockGroup->GetOracle()->GetReadRequestTimeout())
    , ReadHint(std::move(readHint))
{
    ReadHint.Lock.Arm();
}

TReadSingleLocationRequestExecutor::~TReadSingleLocationRequestExecutor()
{
    if (!Promise.IsReady()) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Reply not sent.",
            LogTitle.GetWithTime().c_str());

        Y_ABORT_UNLESS(false);
    }
}

void TReadSingleLocationRequestExecutor::Run()
{
    ScheduleRequestTimeout();

    StartReading();
}

TString TReadSingleLocationRequestExecutor::Print()
{
    TStringBuilder result;
    result << LogTitle.GetWithTime();
    result << "c:" << ReadHint.HostMask.Print();
    result << ",r:" << Requested.Print();
    result << ",f:" << Failed.Print();
    result << (Promise.IsReady() ? ",Replied" : ",NotReplied");

    return result;
}

NThreading::TFuture<IReadRequestExecutor::TResponse>
TReadSingleLocationRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TReadSingleLocationRequestExecutor::StartReading()
{
    if (Promise.IsReady()) {
        return;
    }

    auto candidates = ReadHint.HostMask.Exclude(Requested);

    auto host = candidates.First();
    if (!host) {
        if (Requested == Failed) {
            Reply(MakeError(
                E_REJECTED,
                TStringBuilder() << "Can't read. r:" << Requested.Print()
                                 << ",f:" << Failed.Print()));
        } else {
            LOG_DEBUG(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "%s Read started from all available hosts. r:%s,f:%s",
                LogTitle.GetWithTime().c_str(),
                Requested.Print().c_str(),
                Failed.Print().c_str());
        }
        return;
    }
    Requested.Set(*host);

    const bool fromDDisk = ReadHint.Lsn == 0;

    const size_t tryCount = Requested.Count();
    NActors::NLog::EPriority printPriority =
        tryCount == 1 ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_TRACE;
    if (!Failed.Empty()) {
        printPriority = NActors::NLog::PRI_INFO;
    }

    LOG_LOG(
        *ActorSystem,
        printPriority,
        NKikimrServices::NBS_PARTITION,
        "%s Will read from %s of %s, try %lu",
        LogTitle.GetWithTime().c_str(),
        fromDDisk ? "DDisk" : "PBuffer",
        PrintHostIndex(*host).c_str(),
        tryCount);

    auto onReadResponse = [self = shared_from_this(), host = *host]   //
        (const NThreading::TFuture<TDBGReadBlocksResponse>& f)
    {
        self->OnReadResponse(host, f.GetValue());
    };

    auto future = fromDDisk ? DirectBlockGroup->ReadBlocksFromDDisk(
                                  VChunkConfig.GetVChunkIndex(),
                                  *host,
                                  ReadHint.VChunkRange,
                                  Request->Sglist,
                                  TraceId)
                            : DirectBlockGroup->ReadBlocksFromPBuffer(
                                  VChunkConfig.GetVChunkIndex(),
                                  *host,
                                  ReadHint.Lsn,
                                  ReadHint.VChunkRange,
                                  Request->Sglist,
                                  TraceId);
    future.Subscribe(std::move(onReadResponse));

    ScheduleHedging(DirectBlockGroup->GetOracle()->GetReadHedgingDelay(
        *host,
        ReadHint.Lsn == 0 ? EDataLocation::DDisk : EDataLocation::PBuffer));
}

void TReadSingleLocationRequestExecutor::OnReadResponse(
    THostIndex host,
    const TDBGReadBlocksResponse& response)
{
    if (!HasError(response.Error)) {
        Reply(response.Error);
        return;
    }
    Failed.Set(host);

    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s %s: %s, r:%s,f:%s",
        LogTitle.GetWithTime().c_str(),
        PrintHostIndex(host).c_str(),
        FormatError(response.Error).c_str(),
        Requested.Print().c_str(),
        Failed.Print().c_str());

    StartReading();
}

void TReadSingleLocationRequestExecutor::Reply(NProto::TError error)
{
    if (Promise.IsReady()) {
        return;
    }

    if (HasError(error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());
    } else {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s OK",
            LogTitle.GetWithTime().c_str());
    }

    ReadHint.Lock.Disarm();
    Request->Sglist.Close();

    Promise.TrySetValue(TResponse{.Error = std::move(error)});
}

void TReadSingleLocationRequestExecutor::ScheduleHedging(TDuration hedgingDelay)
{
    if (!hedgingDelay) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Schedule OnHedgingTimeout %s",
        LogTitle.GetWithTime().c_str(),
        FormatDuration(hedgingDelay).c_str());

    DirectBlockGroup->Schedule(
        hedgingDelay,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->OnHedgingTimeout();
            }
        });
}

void TReadSingleLocationRequestExecutor::ScheduleRequestTimeout()
{
    if (!RequestTimeout) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Schedule OnRequestTimeout %s",
        LogTitle.GetWithTime().c_str(),
        FormatDuration(RequestTimeout).c_str());

    DirectBlockGroup->Schedule(
        RequestTimeout,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->OnRequestTimeout();
            }
        });
}

void TReadSingleLocationRequestExecutor::OnHedgingTimeout()
{
    if (Promise.IsReady()) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnHedgingTimeout %s",
        LogTitle.GetWithTime().c_str(),
        Print().c_str());

    const bool allRetriesAreSpent = ReadHint.HostMask == Requested;
    if (!allRetriesAreSpent) {
        StartReading();
    }
}

void TReadSingleLocationRequestExecutor::OnRequestTimeout()
{
    if (Promise.IsReady()) {
        return;
    }

    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnRequestTimeout.",
        LogTitle.GetWithTime().c_str());

    Reply(MakeError(E_TIMEOUT, "Request timeout"));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
