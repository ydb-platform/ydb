#include "erase_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_PARTITION

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TEraseRequestExecutor::TEraseRequestExecutor(
    NActors::TActorSystem* actorSystem,
    const TLogTitle& logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    THostIndex host,
    TEraseHint hint,
    NWilson::TSpan span)
    : ActorSystem(actorSystem)
    , LogTitle(logTitle.GetChildWithTags(
          GetCycleCount(),
          {{"t", "BatchErase"}, {"h", PrintHostIndex(host)}}))
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , Span(std::move(span))
    , Host(host)
    , Hint(std::move(hint))
    , RequestTimeout(DirectBlockGroup->GetOracle()->GetEraseRequestTimeout())
{}

TEraseRequestExecutor::~TEraseRequestExecutor()
{
    if (!Promise.IsReady()) {
        YDB_LOG_ERROR_CTX(*ActorSystem, "Reply not sent",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});

        Y_ABORT_UNLESS(false);
    }
}

void TEraseRequestExecutor::Run()
{
    ScheduleRequestTimeout();

    auto future = DirectBlockGroup->BatchEraseFromPBuffer(
        Host,
        Hint.Segments,
        Span.GetTraceId());
    future.Subscribe(
        [self = shared_from_this()]   //
        (const NThreading::TFuture<TDBGEraseResponse>& f)
        {
            //
            self->OnEraseResponse(f.GetValue());
        });
}

TString TEraseRequestExecutor::Print()
{
    TStringBuilder result;
    result << LogTitle.GetWithTime();
    result << Hint.DebugPrint(true);
    result << (Promise.IsReady() ? ",Replied" : ",NotReplied");
    return result;
}

NThreading::TFuture<TEraseRequestExecutor::TResponse>
TEraseRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TEraseRequestExecutor::OnEraseResponse(const TDBGEraseResponse& response)
{
    if (HasError(response.Error)) {
        YDB_LOG_ERROR_CTX(*ActorSystem, "Erase",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"failed", FormatError(response.Error)});

        Reply({}, MakeLsnVector(Hint.Segments));
        return;
    }

    Reply(MakeLsnVector(Hint.Segments), {});
}

void TEraseRequestExecutor::Reply(
    TVector<ui64> eraseOk,
    TVector<ui64> eraseFailed)
{
    Promise.TrySetValue(TResponse{
        .Host = Host,
        .EraseOk = std::move(eraseOk),
        .EraseFailed = std::move(eraseFailed)});
}

void TEraseRequestExecutor::ScheduleRequestTimeout()
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

void TEraseRequestExecutor::OnRequestTimeout()
{
    if (Promise.IsReady()) {
        return;
    }

    YDB_LOG_WARN_CTX(*ActorSystem, "OnRequestTimeout",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});

    Reply({}, MakeLsnVector(Hint.Segments));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
