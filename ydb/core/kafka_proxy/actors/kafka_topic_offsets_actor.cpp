#include "kafka_topic_offsets_actor.h"

#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>


namespace NKafka {

TTopicOffsetsActor::TTopicOffsetsActor(const TEvKafka::TGetOffsetsRequest& request, const TActorId& requester) 
    : TBase(request, requester)
    , TDescribeTopicActorImpl(NKikimr::NGRpcProxy::V1::TDescribeTopicActorSettings::DescribeTopic(
            true,
            false,
            request.PartitionIds))
{
}

void TTopicOffsetsActor::Bootstrap(const NActors::TActorContext&)
{
    SendDescribeProposeRequest();
    UnsafeBecome(&TTopicOffsetsActor::StateWork);
}

void TTopicOffsetsActor::StateWork(TAutoPtr<IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        default:
        if (!TDescribeTopicActorImpl::StateWork(ev, ActorContext())) {
            TBase::StateWork(ev);
        };
    }
}

void TTopicOffsetsActor::HandleCacheNavigateResponse(
    NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev
) {
    if (!TBase::HandleCacheNavigateResponseBase(ev)) {
        return;
    }
    std::unordered_set<ui32> partititons;
    for (ui32 i = 0; i < PQGroupInfo->Description.PartitionsSize(); i++) {
        auto part = PQGroupInfo->Description.GetPartitions(i).GetPartitionId();
        partititons.insert(part);
    }
    
    for (auto requestedPartition: Settings.Partitions) {
        if (partititons.find(requestedPartition) == partititons.end()) {
            return RaiseError(
                TStringBuilder() << "No partition " << requestedPartition << " in topic",
                Ydb::PersQueue::ErrorCode::BAD_REQUEST, Ydb::StatusIds::SCHEME_ERROR, ActorContext()
            ); 
        }
    }

    ProcessTablets(PQGroupInfo->Description, this->ActorContext());
}

void TTopicOffsetsActor::ApplyResponse(TTabletInfo& tablet, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext&) {
    const auto& record = ev->Get()->Record;
    for (auto i = 0u; i < record.PartResultSize(); i++) {
        const auto& part = record.GetPartResult(i);
        TEvKafka::TPartitionOffsetsInfo resultPartititon;
        resultPartititon.PartitionId = part.GetPartition();
        resultPartititon.StartOffset = part.GetStartOffset();
        resultPartititon.EndOffset = part.GetEndOffset();
        resultPartititon.Generation = tablet.Generation;
        Response->Partitions.emplace_back(std::move(resultPartititon));
    }
}

void TTopicOffsetsActor::Reply(const TActorContext&) {
    this->RespondWithCode(Ydb::StatusIds::SUCCESS);
}

void TTopicOffsetsActor::RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext&) {
    this->AddIssue(NKikimr::NGRpcProxy::V1::FillIssue(error, errorCode));
    this->RespondWithCode(status);
}

}// namespace NKafka
