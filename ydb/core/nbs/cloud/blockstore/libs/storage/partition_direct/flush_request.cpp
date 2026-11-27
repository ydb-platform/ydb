#include "flush_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_PARTITION

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TFlushRequestExecutor::TFlushRequestExecutor(
    NActors::TActorSystem* actorSystem,
    const TLogTitle& logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    THostRoute route,
    TFlushHint hint,
    NWilson::TSpan span)
    : ActorSystem(actorSystem)
    , LogTitle(logTitle.GetChildWithTags(
          GetCycleCount(),
          {{"t", "Flush"},
           {"src",
            THostAndNodeId{
                .HostIndex = route.SourceHostIndex,
                .NodeId = directBlockGroup->GetNodeId(route.SourceHostIndex)}},
           {"dst",
            THostAndNodeId{
                .HostIndex = route.DestinationHostIndex,
                .NodeId =
                    directBlockGroup->GetNodeId(route.DestinationHostIndex)}}}))
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , Span(std::move(span))
    , Route(route)
    , Hint(std::move(hint))
    , Cooldown(DirectBlockGroup->GetOracle()->GetFlushRequestCooldown(
          THostMask::MakeFromRoute(Route)))
    , RequestTimeout(DirectBlockGroup->GetOracle()->GetFlushRequestTimeout())
{
    Y_ABORT_UNLESS(Route.SourceHostIndex != InvalidHostIndex);
    Y_ABORT_UNLESS(Route.DestinationHostIndex != InvalidHostIndex);
}

TFlushRequestExecutor::~TFlushRequestExecutor()
{
    if (!Promise.IsReady()) {
        YDB_LOG_ERROR_CTX(*ActorSystem, "Reply not sent",
            {"withTime", LogTitle.GetWithTime()});

        Y_ABORT_UNLESS(false);
    }
}

void TFlushRequestExecutor::Run()
{
    if (!Cooldown) {
        DoRun();
    } else {
        YDB_LOG_INFO_CTX(*ActorSystem, "Flush cooldown time",
            {"withTime", LogTitle.GetWithTime()},
            {"cooldown", FormatDuration(Cooldown)});
        DirectBlockGroup->Schedule(
            Cooldown,
            [self = shared_from_this()]()
            {
                self->DoRun();   //
            });
    }
}

TString TFlushRequestExecutor::Print()
{
    TStringBuilder result;
    result << LogTitle.GetWithTime();
    result << Hint.DebugPrint(true);
    if (Cooldown) {
        result << ",Cooldown=" << FormatDuration(Cooldown);
    }
    result << (Promise.IsReady() ? ",Replied" : ",NotReplied");
    return result;
}

NThreading::TFuture<TFlushRequestExecutor::TResponse>
TFlushRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TFlushRequestExecutor::DoRun()
{
    ScheduleRequestTimeout();

    auto future = DirectBlockGroup->SyncWithPBuffer(
        VChunkConfig.GetVChunkIndex(),
        Route.SourceHostIndex,
        Route.DestinationHostIndex,
        Hint.Segments,
        Span.GetTraceId());
    future.Subscribe(
        [self = shared_from_this()]   //
        (const NThreading::TFuture<TDBGFlushResponse>& f)
        {
            self->OnFlushResponse(f.GetValue());   //
        });
}

void TFlushRequestExecutor::OnFlushResponse(const TDBGFlushResponse& response)
{
    Y_ABORT_UNLESS(Hint.Segments.size() == response.Errors.size());

    TVector<TPBufferKey> flushOk;
    TVector<TPBufferKey> flushFailed;
    flushOk.reserve(Hint.Segments.size());
    for (size_t i = 0; i < Hint.Segments.size(); ++i) {
        if (HasError(response.Errors[i])) {
            YDB_LOG_ERROR_CTX(*ActorSystem, "Flush",
                {"withTime", LogTitle.GetWithTime()},
                {"bufferKey", Hint.Segments[i].PBufferKey.Print()},
                {"range", Hint.Segments[i].Range.Print()},
                {"error", FormatError(response.Errors[i])});

            flushFailed.push_back(Hint.Segments[i].PBufferKey);
        } else {
            flushOk.push_back(Hint.Segments[i].PBufferKey);
        }
    }

    Reply(std::move(flushOk), std::move(flushFailed));
}

void TFlushRequestExecutor::Reply(
    TVector<TPBufferKey> flushOk,
    TVector<TPBufferKey> flushFailed)
{
    Promise.TrySetValue(TResponse{
        .Route = Route,
        .FlushOk = std::move(flushOk),
        .FlushFailed = std::move(flushFailed)});
}

void TFlushRequestExecutor::ScheduleRequestTimeout()
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

void TFlushRequestExecutor::OnRequestTimeout()
{
    if (Promise.IsReady()) {
        return;
    }

    YDB_LOG_WARN_CTX(*ActorSystem, "OnRequestTimeout",
        {"withTime", LogTitle.GetWithTime()});

    Reply({}, MakePBufferKeys(Hint.Segments));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
