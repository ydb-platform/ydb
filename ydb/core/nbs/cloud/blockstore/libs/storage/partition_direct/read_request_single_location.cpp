#include "read_request_single_location.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

TReadHint ArmLocks(TReadHint readHint)
{
    for (auto& hint: readHint.RangeHints) {
        hint.Lock.Arm();
    }
    return readHint;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TReadSingleLocationRequestExecutor::TReadSingleLocationRequestExecutor(
    NActors::TActorSystem const* actorSystem,
    const TLogTitle& logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TReadHint readHint,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
    : ActorSystem(actorSystem)
    , LogTitle(logTitle.GetChildWithTags(
          GetCycleCount(),
          {{"t", "Read"},
           {"r", request->Headers.Range.Print()},
           {"vr", readHint.RangeHints[0].VChunkRange.Print()}}))
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , ReadHint(ArmLocks(std::move(readHint)))
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
    , HedgingDelay(DirectBlockGroup->GetOracle()->GetReadHedgingDelay())
    , RequestTimeout(DirectBlockGroup->GetOracle()->GetReadRequestTimeout())
{}

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
    Y_ABORT_UNLESS(ReadHint.RangeHints.size() == 1);

    ScheduleRequestTimeout();

    StartReading();
}

TString TReadSingleLocationRequestExecutor::Print()
{
    const auto& hint = ReadHint.RangeHints[0];
    TStringBuilder result;
    result << LogTitle.GetWithTime();
    result << " lsn:" << hint.Lsn;
    result << ",c:" << hint.HostMask.Print();
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

    const auto& hint = ReadHint.RangeHints[0];

    auto candidates = hint.HostMask.Exclude(Requested);

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

    const bool fromDDisk = hint.Lsn == 0;

    LOG_LOG(
        *ActorSystem,
        Requested.Count() == 1 ? NActors::NLog::PRI_DEBUG
                               : NActors::NLog::PRI_INFO,
        NKikimrServices::NBS_PARTITION,
        "%s Will read from %s of %s",
        LogTitle.GetWithTime().c_str(),
        fromDDisk ? "DDisk" : "PBuffer",
        PrintHostIndex(*host).c_str());

    auto onReadResponse = [self = shared_from_this(), host = *host]   //
        (const NThreading::TFuture<TDBGReadBlocksResponse>& f)
    {
        self->OnReadResponse(host, f.GetValue());
    };

    auto future = fromDDisk ? DirectBlockGroup->ReadBlocksFromDDisk(
                                  VChunkConfig.GetVChunkIndex(),
                                  *host,
                                  hint.VChunkRange,
                                  Request->Sglist,
                                  TraceId)
                            : DirectBlockGroup->ReadBlocksFromPBuffer(
                                  VChunkConfig.GetVChunkIndex(),
                                  *host,
                                  hint.Lsn,
                                  hint.VChunkRange,
                                  Request->Sglist,
                                  TraceId);
    future.Subscribe(std::move(onReadResponse));

    ScheduleHedging();
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

    Request->Sglist.Close();

    Promise.TrySetValue(TResponse{.Error = std::move(error)});
}

void TReadSingleLocationRequestExecutor::ScheduleHedging()
{
    if (!HedgingDelay) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Schedule OnHedgingTimeout %s",
        LogTitle.GetWithTime().c_str(),
        FormatDuration(HedgingDelay).c_str());

    DirectBlockGroup->Schedule(
        HedgingDelay,
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

    const auto& hint = ReadHint.RangeHints[0];
    const bool allRetriesAreSpent = hint.HostMask == Requested;
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
