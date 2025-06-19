#include "local_partition_committer.h"
#include "local_proxy.h"
#include "logging.h"

#include <ydb/core/persqueue/writer/common.h>

namespace NKikimr::NReplication {

TLocalTopicPartitionCommitActor::TLocalTopicPartitionCommitActor(
        const TActorId& parent,
        const std::string& database,
        std::string&& topicName,
        ui64 partitionId,
        std::string&& consumerName,
        std::optional<std::string>&& readSessionId,
        ui64 offset)
    : TBaseLocalTopicPartitionActor(database, std::move(topicName), partitionId)
    , Parent(parent)
    , ConsumerName(std::move(consumerName))
    , ReadSessionId(std::move(readSessionId))
    , Offset(offset)
{
}

void TLocalTopicPartitionCommitActor::OnDescribeFinished() {
    DoCommitOffset();
}

void TLocalTopicPartitionCommitActor::OnError(const TString& error) {
    Send(Parent, CreateResponse(NYdb::EStatus::UNAVAILABLE, error));
    PassAway();
}

void TLocalTopicPartitionCommitActor::OnFatalError(const TString& error) {
    Send(Parent, CreateResponse(NYdb::EStatus::SCHEME_ERROR, error));
    PassAway();
}

TString TLocalTopicPartitionCommitActor::MakeLogPrefix() {
    return TStringBuilder() << "Committer[" << SelfId() << ":/" << Database << TopicPath <<" ] ";
}

std::unique_ptr<TEvYdbProxy::TEvCommitOffsetResponse> TLocalTopicPartitionCommitActor::CreateResponse(NYdb::EStatus status, const TString& error) {
    NYdb::NIssue::TIssues issues;
    if (error) {
        issues.AddIssue(error);
    }
    return std::make_unique<TEvYdbProxy::TEvCommitOffsetResponse>(NYdb::TStatus(status, std::move(issues)));
}

STATEFN(TLocalTopicPartitionCommitActor::OnInitEvent) {
    Y_UNUSED(ev);
}

void TLocalTopicPartitionCommitActor::DoCommitOffset() {
    LOG_T("DoCommit");

    NTabletPipe::SendData(SelfId(), PartitionPipeClient, CreateCommitRequest().release());
    Become(&TLocalTopicPartitionCommitActor::StateCommitOffset);
}

void TLocalTopicPartitionCommitActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    const auto& record = ev->Get()->Record;

    TString error;
    if (!NPQ::BasicCheck(record, error, false)) {
        return OnError(TStringBuilder() << "Wrong commit response: " << error);
    }

    Send(Parent, CreateResponse(NYdb::EStatus::SUCCESS, ""));

    PassAway();
}

std::unique_ptr<TEvPersQueue::TEvRequest> TLocalTopicPartitionCommitActor::CreateCommitRequest() const {
    auto request = std::make_unique<TEvPersQueue::TEvRequest>();

    auto& req = *request->Record.MutablePartitionRequest();
    req.SetPartition(PartitionId);
    auto& commit = *req.MutableCmdSetClientOffset();
    commit.SetOffset(Offset);
    commit.SetClientId(ConsumerName);

    return request;
}

STATEFN(TLocalTopicPartitionCommitActor::StateCommitOffset) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvResponse, Handle);

        hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    default:
        OnInitEvent(ev);
    }
}


void TLocalProxyActor::Handle(TEvYdbProxy::TEvCommitOffsetRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto args = std::move(ev->Get()->GetArgs());
    auto& [topicName, partitionId, consumerName, offset, settings] = args;

    auto* actor = new TLocalTopicPartitionCommitActor(ev->Sender, Database, std::move(topicName), partitionId, std::move(consumerName), std::move(settings.ReadSessionId_), offset);
    RegisterWithSameMailbox(actor);
}

}
