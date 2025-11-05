#pragma once

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>


namespace NKikimr::NPQ {


class TMirrorDescriber : public TBaseTabletActor<TMirrorDescriber>, private TConstantLogPrefix {
private:
    static constexpr TDuration INIT_INTERVAL_MAX = TDuration::Seconds(240);
    static constexpr TDuration INIT_INTERVAL_START = TDuration::Seconds(1);

    static constexpr TDuration DESCRIBE_RETRY_TIMEOUT_MAX = TDuration::Seconds(240);
    static constexpr TDuration DESCRIBE_RETRY_TIMEOUT_START = TDuration::Seconds(1);
private:

    STFUNC(StateInit)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQ::TEvInitCredentials, HandleInitCredentials);
            HFunc(TEvPQ::TEvCredentialsCreated, HandleCredentialsCreated);
            HFunc(TEvPQ::TEvChangePartitionConfig, HandleChangeConfig);
            HFunc(TEvents::TEvPoisonPill, Handle);
        default:
            break;
        };
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQ::TEvChangePartitionConfig, HandleChangeConfig);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            HFunc(TEvents::TEvPoisonPill, Handle);
            HFunc(TEvPQ::TEvMirrorTopicDescription, HandleDescriptionResult);
        default:
            break;
        };
    }

private:
    template<class TEvent>
    void ScheduleWithIncreasingTimeout(const TActorId& recipient, TDuration& timeout, const TDuration& maxTimeout, const TActorContext &ctx) {
        ctx.Schedule(timeout, std::make_unique<IEventHandle>(recipient, SelfId(), new TEvent()));
        timeout = Min(timeout * 2, maxTimeout);
    }

    void ScheduleDescription(const TActorContext& ctx);
    void StartInit(const TActorContext& ctx);


    void DescribeTopic(const TActorContext& ctx);

    TString BuildLogPrefix() const override;
    TString GetCurrentState() const;

public:
    TMirrorDescriber(
        ui64 tabletId,
        TActorId readBalancerActorId,
        TString topicName,
        const NKikimrPQ::TMirrorPartitionConfig& config
    );
    void Bootstrap(const TActorContext& ctx);
    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);
    void HandleChangeConfig(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx);
    void HandleInitCredentials(TEvPQ::TEvInitCredentials::TPtr& ev, const TActorContext& ctx);
    void HandleCredentialsCreated(TEvPQ::TEvCredentialsCreated::TPtr& ev, const TActorContext& ctx);
    void HandleWakeup(const TActorContext& ctx);
    void HandleDescriptionResult(TEvPQ::TEvMirrorTopicDescription::TPtr& ev, const TActorContext& ctx);
private:
    TString TopicName;
    NKikimrPQ::TMirrorPartitionConfig Config;


    NYdb::TCredentialsProviderFactoryPtr CredentialsProvider;

    TDuration CredentialsInitInterval = INIT_INTERVAL_START;
    TDuration DescribeRetryTimeout = DESCRIBE_RETRY_TIMEOUT_START;

    bool CredentialsRequestInFlight = false;
    bool DescribeTopicRequestInFlight = false;
};

}// NKikimr
