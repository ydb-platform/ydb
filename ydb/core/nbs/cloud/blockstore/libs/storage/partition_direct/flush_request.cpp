#include "flush_request.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TFlushRequestExecutor::TFlushRequestExecutor(
    NActors::TActorSystem* actorSystem,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    THostRoute route,
    TFlushHint hint,
    NWilson::TSpan span)
    : ActorSystem(actorSystem)
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , Span(std::move(span))
    , Route(route)
    , Hint(std::move(hint))
{
    Y_ABORT_UNLESS(Route.SourceHostIndex != InvalidHostIndex);
    Y_ABORT_UNLESS(Route.DestinationHostIndex != InvalidHostIndex);
}

TFlushRequestExecutor::~TFlushRequestExecutor()
{
    if (!Promise.IsReady()) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TFlushRequestExecutor. Reply not sent");

        Y_ABORT_UNLESS(false);
    }
}

void TFlushRequestExecutor::Run()
{
    auto future = DirectBlockGroup->SyncWithPBuffer(
        VChunkConfig.VChunkIndex,
        Route.SourceHostIndex,
        Route.DestinationHostIndex,
        Hint.Segments,
        Span.GetTraceId());
    future.Subscribe(
        [self = shared_from_this()]   //
        (const NThreading::TFuture<TDBGFlushResponse>& f)
        {
            //
            self->OnFlushResponse(f.GetValue());
        });
}

NThreading::TFuture<TFlushRequestExecutor::TResponse>
TFlushRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TFlushRequestExecutor::OnFlushResponse(const TDBGFlushResponse& response)
{
    Y_ABORT_UNLESS(Hint.Segments.size() == response.Errors.size());

    TVector<ui64> flushOk;
    TVector<ui64> flushFailed;
    flushOk.reserve(Hint.Segments.size());
    for (size_t i = 0; i < Hint.Segments.size(); ++i) {
        if (HasError(response.Errors[i])) {
            LOG_ERROR(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "TFlushRequestExecutor. Flush failed: %lu %s %s",
                Hint.Segments[i].Lsn,
                Hint.Segments[i].Range.Print().c_str(),
                FormatError(response.Errors[i]).c_str());

            flushFailed.push_back(Hint.Segments[i].Lsn);
        } else {
            flushOk.push_back(Hint.Segments[i].Lsn);
        }
    }

    Reply(std::move(flushOk), std::move(flushFailed));
}

void TFlushRequestExecutor::Reply(
    TVector<ui64> flushOk,
    TVector<ui64> flushFailed)
{
    Promise.TrySetValue(TResponse{
        .Route = Route,
        .FlushOk = std::move(flushOk),
        .FlushFailed = std::move(flushFailed)});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
