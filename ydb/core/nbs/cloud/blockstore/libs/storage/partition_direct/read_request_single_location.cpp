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
    , HedgingDelay(DirectBlockGroup->GetOracle()->GetReadHedgingDelay())
    , RequestTimeout(DirectBlockGroup->GetOracle()->GetReadRequestTimeout())
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
            YDB_LOG_DEBUG_CTX(*ActorSystem, "Read started from all available hosts",
                {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
                {"r", Requested.Print()},
                {"#_,f", Failed.Print()});
        }
        return;
    }
    Requested.Set(*host);

    const bool fromDDisk = ReadHint.Lsn == 0;

    YDB_LOG_CTX(*ActorSystem, Requested.Count() == 1 ? NActors::NLog::PRI_DEBUG
                               : NActors::NLog::PRI_INFO, "Will read from of",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_num_0", fromDDisk ? "DDisk" : "PBuffer"},
        {"#_PrintHostIndex(*host).c_str", PrintHostIndex(*host)});

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

    YDB_LOG_WARN_CTX(*ActorSystem, "",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_PrintHostIndex(host).c_str", PrintHostIndex(host)},
        {"#_FormatError(response.Error).c_str", FormatError(response.Error)},
        {"r", Requested.Print()},
        {"#_,f", Failed.Print()});

    StartReading();
}

void TReadSingleLocationRequestExecutor::Reply(NProto::TError error)
{
    if (Promise.IsReady()) {
        return;
    }

    if (HasError(error)) {
        YDB_LOG_ERROR_CTX(*ActorSystem, "",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"error", FormatError(error)});
    } else {
        YDB_LOG_DEBUG_CTX(*ActorSystem, "OK",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});
    }

    ReadHint.Lock.Disarm();
    Request->Sglist.Close();

    Promise.TrySetValue(TResponse{.Error = std::move(error)});
}

void TReadSingleLocationRequestExecutor::ScheduleHedging()
{
    if (!HedgingDelay) {
        return;
    }

    YDB_LOG_DEBUG_CTX(*ActorSystem, "Schedule OnHedgingTimeout",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_FormatDuration(HedgingDelay).c_str", FormatDuration(HedgingDelay)});

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
        {"#_Print().c_str", Print()});

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
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});

    Reply(MakeError(E_TIMEOUT, "Request timeout"));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
