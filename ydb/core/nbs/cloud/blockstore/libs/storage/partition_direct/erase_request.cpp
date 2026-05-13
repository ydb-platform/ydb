#include "erase_request.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TEraseRequestExecutor::TEraseRequestExecutor(
    NActors::TActorSystem* actorSystem,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    THostIndex host,
    TEraseHint hint,
    NWilson::TSpan span)
    : ActorSystem(actorSystem)
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
            "TEraseRequestExecutor. Reply not sent");

        Y_ABORT_UNLESS(false);
    }
}

void TEraseRequestExecutor::Run()
{
    auto future = DirectBlockGroup->EraseFromPBuffer(
        VChunkConfig.VChunkIndex,
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

NThreading::TFuture<TEraseRequestExecutor::TResponse>
TEraseRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TEraseRequestExecutor::OnEraseResponse(const TDBGEraseResponse& response)
{
    TVector<ui64> lsns;
    lsns.reserve(Hint.Segments.size());
    for (const auto& segment: Hint.Segments) {
        lsns.push_back(segment.Lsn);
    }

    if (HasError(response.Error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TEraseRequestExecutor. Erase failed: %s",
            FormatError(response.Error).c_str());

        Reply({}, std::move(lsns));
        return;
    }

    Reply(std::move(lsns), {});
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
