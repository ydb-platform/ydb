#pragma once

#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NReplication {

using namespace NActors;
using namespace NSchemeCache;

enum class EWakeupType : ui64 {
    Describe,
    InitOffset
};

class TBaseLocalTopicPartitionActor : public TActorBootstrapped<TBaseLocalTopicPartitionActor>
                                    , private TSchemeCacheHelpers {

    using TThis = TBaseLocalTopicPartitionActor;
    static constexpr size_t MaxAttempts = 5;

public:
    TBaseLocalTopicPartitionActor(const std::string& database, const std::string&& topicName, const ui32 partitionId);
    void Bootstrap();

protected:
    virtual void OnDescribeFinished() = 0;
    virtual void OnError(const TString& error) = 0;
    virtual void OnFatalError(const TString& error) = 0;
    virtual STATEFN(OnInitEvent) = 0;

protected:
    void DoDescribe();

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void HandleOnDescribe(TEvents::TEvWakeup::TPtr& ev);

    TCheckFailFunc DoRetryDescribe();
    TCheckFailFunc LeaveOnError();

    STATEFN(StateDescribe);

protected:
    void DoCreatePipe();
    void CreatePipe();

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev);

    STATEFN(StateCreatePipe);

protected:
    void PassAway();

protected:
    const std::string Database;
    const TString TopicName;
    const ui32 PartitionId;

    ui64 PartitionTabletId;
    TActorId PartitionPipeClient;

    size_t Attempt = 0;
};

}
