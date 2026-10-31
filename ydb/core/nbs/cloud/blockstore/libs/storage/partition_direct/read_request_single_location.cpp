#include "read_request_single_location.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_PARTITION

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto SlowRequestTime = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

}   // namespace

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
           {"pBufferKey", readHint.PBufferKey.Print()},
           {"r", request->Headers.Range.Print()},
           {"vr", readHint.VChunkRange.Print()}}))
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
    , RequestTimeout(DirectBlockGroup->GetOracle()->GetReadRequestTimeout())
    , SgList(Request->Sglist.CreateDepender())
    , ReadHint(std::move(readHint))
{
    ReadHint.Lock.Arm();
}

TReadSingleLocationRequestExecutor::~TReadSingleLocationRequestExecutor()
{
    if (!Promise.IsReady()) {
        YDB_LOG_ERROR_CTX(*ActorSystem, "Reply not sent",
            {"withTime", LogTitle.GetWithTime()});

        Y_ABORT_UNLESS(false);
    }
}

void TReadSingleLocationRequestExecutor::Run()
{
    StartAt = TInstant::Now();
    ScheduleRequestTimeout();

    StartReading();
}

TString TReadSingleLocationRequestExecutor::Print()
{
    TStringBuilder result;
    result << LogTitle.GetWithTime() << " " << ExtendedDebugState() << " "
           << (Promise.IsReady() ? ",Replied" : ",NotReplied");

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
            TStringBuilder message;
            message << "Read from all available hosts failed. "
                    << ExtendedDebugState();
            YDB_LOG_ERROR_CTX(*ActorSystem, "",
                {"withTime", LogTitle.GetWithTime()},
                {"message", message});
            Reply(MakeError(E_REJECTED, std::move(message)));
        } else {
            YDB_LOG_DEBUG_CTX(*ActorSystem, "Read started from all available hosts",
                {"withTime", LogTitle.GetWithTime()},
                {"extendedDebugState", ExtendedDebugState()});
        }
        return;
    }
    Requested.Set(*host);

    const bool fromDDisk = ReadHint.PBufferKey.Lsn == 0;

    const size_t tryCount = Requested.Count();
    NActors::NLog::EPriority printPriority =
        tryCount == 1 ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_TRACE;
    if (!Failed.Empty()) {
        printPriority = NActors::NLog::PRI_INFO;
    }

    YDB_LOG_CTX(*ActorSystem, printPriority, "Will read",
        {"withTime", LogTitle.GetWithTime()},
        {"from", fromDDisk ? "DDisk" : "PBuffer"},
        {"hostAndNode", PrintHostAndNode(*host)},
        {"tryCount", tryCount});

    ScheduleHedging(DirectBlockGroup->GetOracle()->GetReadHedgingDelay(
        *host,
        ReadHint.PBufferKey.Lsn == 0 ? EDataLocation::DDisk
                                     : EDataLocation::PBuffer));

    auto future = fromDDisk ? DirectBlockGroup->ReadBlocksFromDDisk(
                                  VChunkConfig.GetVChunkIndex(),
                                  *host,
                                  ReadHint.VChunkRange,
                                  SgList,
                                  TraceId)
                            : DirectBlockGroup->ReadBlocksFromPBuffer(
                                  VChunkConfig.GetVChunkIndex(),
                                  *host,
                                  ReadHint.PBufferKey,
                                  ReadHint.VChunkRange,
                                  SgList,
                                  TraceId);
    future.Subscribe(
        [self = shared_from_this(), host = *host]   //
        (const NThreading::TFuture<TDBGReadBlocksResponse>& f)
        {
            self->OnReadResponse(host, f.GetValue());   //
        });
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

    if (Promise.IsReady()) {
        return;
    }

    YDB_LOG_WARN_CTX(*ActorSystem, "Response from",
        {"withTime", LogTitle.GetWithTime()},
        {"hostAndNode", PrintHostAndNode(host)},
        {"error", FormatError(response.Error)},
        {"extendedDebugState", ExtendedDebugState()});

    StartReading();
}

void TReadSingleLocationRequestExecutor::Reply(NProto::TError error)
{
    if (Promise.IsReady()) {
        return;
    }

    const auto duration = TInstant::Now() - StartAt;
    if (duration > SlowRequestTime) {
        YDB_LOG_INFO_CTX(*ActorSystem, "[?] Slow request",
            {"withTime", LogTitle.GetWithTime()},
            {"extendedDebugState", ExtendedDebugState()});
    }

    if (HasError(error)) {
        YDB_LOG_ERROR_CTX(*ActorSystem, "[!] Reply",
            {"withTime", LogTitle.GetWithTime()},
            {"error", FormatError(error)},
            {"extendedDebugState", ExtendedDebugState()});
    } else {
        YDB_LOG_DEBUG_CTX(*ActorSystem, "Reply OK",
            {"withTime", LogTitle.GetWithTime()},
            {"extendedDebugState", ExtendedDebugState()});
    }

    ReadHint.Lock.Disarm();
    SgList.Close();

    Promise.TrySetValue(TResponse{.Error = std::move(error)});
}

void TReadSingleLocationRequestExecutor::ScheduleHedging(TDuration hedgingDelay)
{
    if (!hedgingDelay) {
        return;
    }

    YDB_LOG_DEBUG_CTX(*ActorSystem, "Schedule OnHedgingTimeout",
        {"withTime", LogTitle.GetWithTime()},
        {"extendedDebugState", ExtendedDebugState()},
        {"hedgingDelay", FormatDuration(hedgingDelay)});

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

    YDB_LOG_DEBUG_CTX(*ActorSystem, "Schedule OnRequestTimeout",
        {"withTime", LogTitle.GetWithTime()},
        {"duration", FormatDuration(RequestTimeout)});

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

    YDB_LOG_DEBUG_CTX(*ActorSystem, "OnHedgingTimeout",
        {"withTime", LogTitle.GetWithTime()},
        {"extendedDebugState", ExtendedDebugState()});

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

    YDB_LOG_WARN_CTX(*ActorSystem, "OnRequestTimeout",
        {"withTime", LogTitle.GetWithTime()},
        {"extendedDebugState", ExtendedDebugState()});

    Reply(MakeError(E_TIMEOUT, "Request timeout"));
}

TString TReadSingleLocationRequestExecutor::ExtendedDebugState() const
{
    TStringBuilder result;
    result << "a:" << ReadHint.HostMask.Print();
    result << ",r:" << Requested.Print();
    result << ",f:" << Failed.Print();
    return result;
}

TString TReadSingleLocationRequestExecutor::PrintHostAndNode(
    THostIndex host) const
{
    return PrintHostAndNodeId(host, DirectBlockGroup->GetNodeId(host));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
