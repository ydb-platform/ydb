#pragma once

#include "local_partition_actor.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

namespace NKikimr::NReplication {

class TLocalTopicPartitionCommitActor : public TBaseLocalTopicPartitionActor {
    using TBase = TBaseLocalTopicPartitionActor;

public:
    TLocalTopicPartitionCommitActor(
        const TActorId& parent,
        const std::string& database,
        std::string&& topicName,
        ui64 partitionId,
        std::string&& consumerName,
        std::optional<std::string>&& readSessionId,
        ui64 offset);

protected:
    void OnDescribeFinished() override;
    void OnError(const TString& error) override;
    void OnFatalError(const TString& error) override;
    TString MakeLogPrefix() override;

    std::unique_ptr<TEvYdbProxy::TEvCommitOffsetResponse> CreateResponse(NYdb::EStatus status, const TString& error);

    STATEFN(OnInitEvent) override;

private:
    void DoCommitOffset();

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev);

    std::unique_ptr<TEvPersQueue::TEvRequest> CreateCommitRequest() const;

    STATEFN(StateCommitOffset);

private:
    const TActorId Parent;
    const TString ConsumerName;
    const std::optional<TString> ReadSessionId;
    const ui64 Offset;
};

}
