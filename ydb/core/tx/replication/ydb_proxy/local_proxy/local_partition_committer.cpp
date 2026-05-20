#include "local_partition_actor.h"
#include "local_proxy.h"
#include "logging.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/writer/common.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

namespace NKikimr::NReplication {

class TLocalTopicPartitionCommitActor: public TBaseLocalTopicPartitionActor {
    using TBase = TBaseLocalTopicPartitionActor;

public:
    TLocalTopicPartitionCommitActor(
            const TActorId& parent,
            const std::string& database,
            const std::string& topicName,
            ui64 partitionId,
            std::string&& consumerName,
            ui64 offset)
        : TBaseLocalTopicPartitionActor(database, topicName, partitionId)
        , Parent(parent)
        , ConsumerName(std::move(consumerName))
        , Offset(offset)
    {
    }

protected:
    void OnDescribeFinished() override {
        DoCommitOffset();
    }

    void OnError(const TString& error) override {
        Send(Parent, MakeResponse(NYdb::EStatus::UNAVAILABLE, error));
        PassAway();
    }

    void OnFatalError(const TString& error) override {
        Send(Parent, MakeResponse(NYdb::EStatus::SCHEME_ERROR, error));
        PassAway();
    }

    TString MakeLogPrefix() override {
        return TStringBuilder() << "Committer[" << SelfId() << ":/" << Database << TopicPath <<" ] ";
    }

    static std::unique_ptr<TEvYdbProxy::TEvCommitOffsetResponse> MakeResponse(NYdb::EStatus status, const TString& error) {
        NYdb::NIssue::TIssues issues;
        if (error) {
            issues.AddIssue(error);
        }

        return std::make_unique<TEvYdbProxy::TEvCommitOffsetResponse>(NYdb::TStatus(status, std::move(issues)));
    }

    STATEFN(OnInitEvent) override {
        Y_UNUSED(ev);
    }

private:
    void DoCommitOffset() {
        LOG_T("DoCommit");

        NTabletPipe::SendData(SelfId(), PartitionPipeClient, MakeCommitRequest().release());
        Become(&TLocalTopicPartitionCommitActor::StateCommitOffset);
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        const auto& record = ev->Get()->Record;

        TString error;
        if (!NPQ::BasicCheck(record, error, false)) {
            return OnError(TStringBuilder() << "Wrong commit response: " << error);
        }

        Send(Parent, MakeResponse(NYdb::EStatus::SUCCESS, ""));

        PassAway();
    }

    std::unique_ptr<TEvPersQueue::TEvRequest> MakeCommitRequest() const {
        auto request = std::make_unique<TEvPersQueue::TEvRequest>();

        auto& req = *request->Record.MutablePartitionRequest();
        req.SetPartition(PartitionId);
        auto& commit = *req.MutableCmdSetClientOffset();
        commit.SetOffset(Offset);
        commit.SetClientId(ConsumerName);

        return request;
    }

    STATEFN(StateCommitOffset) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPersQueue::TEvResponse, Handle);

            hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        default:
            OnInitEvent(ev);
        }
    }

private:
    const TActorId Parent;
    const TString ConsumerName;
    const ui64 Offset;

}; // TLocalTopicPartitionCommitActor

void TLocalProxyActor::Handle(TEvYdbProxy::TEvCommitOffsetRequest::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto [topicName, partitionId, consumerName, offset, settings] = std::move(ev->Get()->GetArgs());
    RegisterWithSameMailbox(new TLocalTopicPartitionCommitActor(
        ev->Sender,
        Database,
        topicName,
        partitionId,
        std::move(consumerName),
        offset
    ));
}

}
