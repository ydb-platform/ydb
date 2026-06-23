#include "erase_request.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

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
{}

TEraseRequestExecutor::~TEraseRequestExecutor()
{
    if (!Promise.IsReady()) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Reply not sent",
            LogTitle.GetWithTime().c_str());

        Y_ABORT_UNLESS(false);
    }
}

void TEraseRequestExecutor::Run()
{
    auto future = DirectBlockGroup->BatchEraseFromPBuffer(
        VChunkConfig.GetVChunkIndex(),
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
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Erase failed: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(response.Error).c_str());

        Reply({}, Hint.MakeLsnVector());
        return;
    }

    Reply(Hint.MakeLsnVector(), {});
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

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
