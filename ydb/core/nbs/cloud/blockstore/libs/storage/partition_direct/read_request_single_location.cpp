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
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});

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
                {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
                {"message", message});
            Reply(MakeError(E_REJECTED, std::move(message)));
        } else {
            YDB_LOG_DEBUG_CTX(*ActorSystem, "Read started from all available hosts",
                {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
                {"#_ExtendedDebugState().c_str", ExtendedDebugState()});
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

    YDB_LOG_CTX(*ActorSystem, printPriority, "Will read from of try",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_num_0", fromDDisk ? "DDisk" : "PBuffer"},
        {"#_PrintHostAndNode(*host).c_str", PrintHostAndNode(*host)},
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
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_PrintHostAndNode(host).c_str", PrintHostAndNode(host)},
        {"#_FormatError(response.Error).Quote().c_str", FormatError(response.Error)},
        {"#_ExtendedDebugState().c_str", ExtendedDebugState()});

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
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()});
    }

    if (HasError(error)) {
        YDB_LOG_ERROR_CTX(*ActorSystem, "[!] Reply",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"error", FormatError(error)},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()});
    } else {
        YDB_LOG_DEBUG_CTX(*ActorSystem, "Reply OK",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()});
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
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_ExtendedDebugState().c_str", ExtendedDebugState()},
        {"#_FormatDuration(hedgingDelay).c_str", FormatDuration(hedgingDelay)});

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
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_FormatDuration(RequestTimeout).c_str", FormatDuration(RequestTimeout)});

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
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_ExtendedDebugState().c_str", ExtendedDebugState()});

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
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_ExtendedDebugState().c_str", ExtendedDebugState()});

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
