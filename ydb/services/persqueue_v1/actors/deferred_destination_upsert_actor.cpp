#include "deferred_destination_upsert_actor.h"

#include <ydb/core/persqueue/deferred_publish/destination_blob.h>
#include <ydb/core/persqueue/deferred_publish/events.h>
#include <ydb/core/persqueue/deferred_publish/get_destination_blob_query.h>
#include <ydb/core/persqueue/deferred_publish/upsert_destination_query.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NGRpcProxy::V1 {

namespace {

using namespace NPQ::NDeferredPublish;

class TDeferredDestinationUpsertActor : public NActors::TActorBootstrapped<TDeferredDestinationUpsertActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_PARTITION_WRITER_ACTOR;
    }

    TDeferredDestinationUpsertActor(
        const NActors::TActorId& writer,
        TDeferredDestinationUpsertParams request)
        : Writer(writer)
        , Request(std::move(request))
    {}

    void Bootstrap() {
        StartGetDestinationBlob();
        Become(&TThis::StateGetDestinationBlob);
    }

private:
    static constexpr ui32 MaxUpsertRetries = 3;

    static bool IsRetryable(Ydb::StatusIds::StatusCode status) {
        switch (status) {
        case Ydb::StatusIds::OVERLOADED:
        case Ydb::StatusIds::INTERNAL_ERROR:
        case Ydb::StatusIds::UNAVAILABLE:
            return true;
        default:
            return false;
        }
    }

    void StartGetDestinationBlob() {
        Register(CreateGetDestinationBlobQueryActor(
            SelfId(),
            Request.Database,
            Request.IntPublicationId,
            Request.TopicPath));
    }

    void StartUpsertDestinationQuery() {
        Register(CreateUpsertDestinationQueryActor(
            SelfId(),
            Request.Database,
            Request.IntPublicationId,
            Request.TopicPath,
            PendingUpsertBlob));
    }

    void ReplySuccess() {
        auto* event = new NPQ::TEvPartitionWriter::TEvDeferredDestinationUpsertResult;
        event->Success = true;
        Send(Writer, event);
        PassAway();
    }

    void ReplyFailure(const TString& reason) {
        auto* event = new NPQ::TEvPartitionWriter::TEvDeferredDestinationUpsertResult;
        event->Success = false;
        event->Reason = reason;
        Send(Writer, event);
        PassAway();
    }

    STATEFN(StateGetDestinationBlob) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvGetDestinationBlobResponse, HandleGetDestinationBlobResponse);
        default:
            break;
        }
    }

    void HandleGetDestinationBlobResponse(TEvGetDestinationBlobResponse::TPtr& ev) {
        const auto& response = *ev->Get();
        if (response.Status != Ydb::StatusIds::SUCCESS) {
            if (IsRetryable(response.Status) && RetryCount < MaxUpsertRetries) {
                ++RetryCount;
                StartGetDestinationBlob();
                return;
            }
            return ReplyFailure(response.Issues.ToOneLineString());
        }

        NKikimrPQ::TDeferredPublishDestinationBlob blob;
        if (response.DestinationBlob) {
            if (!ParseDestinationBlob(*response.DestinationBlob, &blob)) {
                return ReplyFailure("Invalid destination blob");
            }
        }

        AddOrUpdateTopicPartition(&blob, Request.PartitionId, Request.TabletId);
        PendingUpsertBlob = SerializeDestinationBlob(blob);
        RetryCount = 0;
        StartUpsertDestinationQuery();
        Become(&TThis::StateUpsertDestination);
    }

    STATEFN(StateUpsertDestination) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvUpsertDestinationResponse, HandleUpsertDestinationResponse);
        default:
            break;
        }
    }

    void HandleUpsertDestinationResponse(TEvUpsertDestinationResponse::TPtr& ev) {
        const auto& response = *ev->Get();
        if (response.Status == Ydb::StatusIds::SUCCESS) {
            return ReplySuccess();
        }

        if (IsRetryable(response.Status) && RetryCount < MaxUpsertRetries) {
            ++RetryCount;
            StartUpsertDestinationQuery();
            return;
        }

        ReplyFailure(response.Issues.ToOneLineString());
    }

    const NActors::TActorId Writer;
    const TDeferredDestinationUpsertParams Request;

    TString PendingUpsertBlob;
    ui32 RetryCount = 0;
};

} // namespace

NActors::IActor* CreateDeferredDestinationUpsertActor(
    const NActors::TActorId& writer,
    TDeferredDestinationUpsertParams params)
{
    return new TDeferredDestinationUpsertActor(writer, std::move(params));
}

} // namespace NKikimr::NGRpcProxy::V1
