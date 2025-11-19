#pragma once

#include "local_partition_actor.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

namespace NKikimr::NReplication {

class TLocalTopicPartitionReaderActor : public TBaseLocalTopicPartitionActor {
    using TBase = TBaseLocalTopicPartitionActor;

    constexpr static TDuration ReadTimeout = TDuration::Seconds(1);
    constexpr static ui64 ReadLimitBytes = 1_MB;

    struct ReadRequest {
        TActorId Sender;
        ui64 Cookie;
        bool SkipCommit;
    };

public:
    TLocalTopicPartitionReaderActor(const TActorId& parent, const std::string& database, TEvYdbProxy::TTopicReaderSettings& settings);

protected:
    void OnDescribeFinished() override;
    void OnError(const TString& error) override;
    void OnFatalError(const TString& error) override;
    TString MakeLogPrefix() override;

    std::unique_ptr<TEvYdbProxy::TEvTopicReaderGone> CreateError(NYdb::EStatus status, const TString& error);

    STATEFN(OnInitEvent) override;

private:
    void HandleInit(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev);
    void Handle(TEvYdbProxy::TEvCommitOffsetRequest::TPtr& ev);

private:
    void DoInitOffset();

    void HandleOnInitOffset(TEvPersQueue::TEvResponse::TPtr& ev);
    void HandleOnInitOffset(TEvents::TEvWakeup::TPtr& ev);

    std::unique_ptr<TEvPersQueue::TEvRequest> CreateGetOffsetRequest() const;

    STATEFN(StateInitOffset);

private:
    void DoWork();

    void Handle(TEvYdbProxy::TEvReadTopicRequest::TPtr& ev);
    void Handle(ReadRequest& request);

    std::unique_ptr<TEvPersQueue::TEvRequest> CreateReadRequest() const;
    std::unique_ptr<TEvPersQueue::TEvRequest> CreateCommitRequest() const;

    STATEFN(StateWork);

private:
    void DoWaitData();

    void HandleOnWaitData(TEvPersQueue::TEvResponse::TPtr& ev);

    STATEFN(StateWaitData);

private:
    const TActorId Parent;
    const TString Consumer;
    const bool AutoCommit;

    std::deque<ReadRequest> RequestsQueue;

    ui64 Offset;
    ui64 SentOffset;
};

}
