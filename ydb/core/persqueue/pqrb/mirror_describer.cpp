#include "mirror_describer.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <google/protobuf/util/message_differencer.h>

#define PQ_ENSURE(condition) AFL_ENSURE(condition)("topic", TopicName)

using namespace NPersQueue;

namespace NKikimr {
namespace NPQ {

TMirrorDescriber::TMirrorDescriber(
    ui64 tabletId,
    TActorId readBalancerActorId,
    TString topicName,
    const NKikimrPQ::TMirrorPartitionConfig& config
)
    : TBaseTabletActor(tabletId, readBalancerActorId, NKikimrServices::PQ_MIRROR_DESCRIBER)
    , TopicName(std::move(topicName))
    , Config(config)
{
}

void TMirrorDescriber::Bootstrap(const TActorContext& ctx) {
    StartInit(ctx);
}

void TMirrorDescriber::StartInit(const TActorContext& ctx) {
    Become(&TThis::StateInit);
    DescribeRetryTimeout = DESCRIBE_RETRY_TIMEOUT_START;
    ctx.Send(SelfId(), new TEvPQ::TEvInitCredentials);
}

void TMirrorDescriber::Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    LOG_N("killed");
    CredentialsProvider = nullptr;
    Die(ctx);
}

void TMirrorDescriber::HandleChangeConfig(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx) {
    bool equalConfigs = google::protobuf::util::MessageDifferencer::Equals(
        Config,
        ev->Get()->Config.GetPartitionConfig().GetMirrorFrom()
    );
    LOG_D("got new config, equal with previous: " << equalConfigs);
    if (!equalConfigs) {
        Config = ev->Get()->Config.GetPartitionConfig().GetMirrorFrom();
        LOG_I("changing config");
        StartInit(ctx);
    }
}

void TMirrorDescriber::HandleDescriptionResult(TEvPQ::TEvMirrorTopicDescription::TPtr& ev, const TActorContext& ctx) {
    DescribeTopicRequestInFlight = false;
    const auto& description = ev->Get()->Description;
    if (!description.has_value()) {
        LOG_E("cannot describe topic " << description.error());
        ScheduleWithIncreasingTimeout<TEvents::TEvWakeup>(SelfId(), DescribeRetryTimeout, DESCRIBE_RETRY_TIMEOUT_MAX, ctx);
        return;
    }
    const NYdb::NTopic::TDescribeTopicResult& result = description.value();
    if (!result.IsSuccess()) {
        LOG_E("cannot describe topic " << result.GetIssues().ToString());
        ScheduleWithIncreasingTimeout<TEvents::TEvWakeup>(SelfId(), DescribeRetryTimeout, DESCRIBE_RETRY_TIMEOUT_MAX, ctx);
        return;
    }
    auto debugTopicDescriptionString = [](const NYdb::NTopic::TTopicDescription& descr) {
        Ydb::Topic::CreateTopicRequest req;
        descr.SerializeTo(req);
        return req.ShortUtf8DebugString();
    };
    LOG_T("topic description: " << debugTopicDescriptionString(description.value().GetTopicDescription()));
    ctx.Send(TabletActorId, ev->Release());
    ctx.Schedule(DESCRIBE_RETRY_TIMEOUT_MAX, new TEvents::TEvWakeup());
}

void TMirrorDescriber::DescribeTopic(const TActorContext& ctx) {
    if (DescribeTopicRequestInFlight) {
        LOG_I("description request already inflight.");
        return;
    }

    auto factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    PQ_ENSURE(factory);
    auto future = factory->GetTopicDescription(Config, CredentialsProvider);
    future.Subscribe(
        [
            actorSystem = ctx.ActorSystem(),
            selfId = SelfId()
        ](const NThreading::TFuture<NYdb::NTopic::TDescribeTopicResult>& result) {
            THolder<TEvPQ::TEvMirrorTopicDescription> ev;
            const bool hasValue = result.HasValue();
            if (hasValue) {
                const auto& value = result.GetValue();
                ev = MakeHolder<TEvPQ::TEvMirrorTopicDescription>(value);
                actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev.Release()));
                return;
            }
            try {
                ev = MakeHolder<TEvPQ::TEvMirrorTopicDescription>(result.GetValue());
            } catch (...) {
                ev = MakeHolder<TEvPQ::TEvMirrorTopicDescription>(CurrentExceptionMessage());
            }
            actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev.Release()));
        }
    );
    DescribeTopicRequestInFlight = true;
}

void TMirrorDescriber::HandleInitCredentials(TEvPQ::TEvInitCredentials::TPtr& /*ev*/, const TActorContext& ctx) {
    if (CredentialsRequestInFlight) {
        LOG_W("credentials request already inflight.");
        return;
    }
    CredentialsProvider = nullptr;

    auto factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    PQ_ENSURE(factory);
    auto future = factory->GetCredentialsProvider(Config.GetCredentials());
    future.Subscribe(
        [
            actorSystem = ctx.ActorSystem(),
            selfId = SelfId()
        ](const NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr>& result) {
            THolder<TEvPQ::TEvCredentialsCreated> ev;
            if (result.HasException()) {
                TString error;
                try {
                    result.TryRethrow();
                } catch(...) {
                    error = CurrentExceptionMessage();
                }
                ev = MakeHolder<TEvPQ::TEvCredentialsCreated>(error);
            } else {
                ev = MakeHolder<TEvPQ::TEvCredentialsCreated>(result.GetValue());
            }
            actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev.Release()));
        }
    );
    CredentialsRequestInFlight = true;
}

void TMirrorDescriber::HandleCredentialsCreated(TEvPQ::TEvCredentialsCreated::TPtr& ev, const TActorContext& ctx) {
    CredentialsRequestInFlight = false;
    if (ev->Get()->Error) {
        LOG_W("cannot initialize credentials provider: " << ev->Get()->Error.value());
        ScheduleWithIncreasingTimeout<TEvPQ::TEvInitCredentials>(SelfId(), CredentialsInitInterval, INIT_INTERVAL_MAX, ctx);
        return;
    }

    CredentialsProvider = ev->Get()->Credentials;
    LOG_N("credentials provider created " << bool(CredentialsProvider));
    CredentialsInitInterval = INIT_INTERVAL_START;
    ScheduleDescription(ctx);
}

void TMirrorDescriber::HandleWakeup(const TActorContext& ctx) {
    DescribeTopic(ctx);
}
void TMirrorDescriber::ScheduleDescription(const TActorContext& ctx) {
    Become(&TThis::StateWork);
    ScheduleWithIncreasingTimeout<TEvents::TEvWakeup>(SelfId(), DescribeRetryTimeout, DESCRIBE_RETRY_TIMEOUT_MAX, ctx);
}

TString TMirrorDescriber::BuildLogPrefix() const {
    return TStringBuilder() << "[MirrorDescriber][" << TopicName << "] ";
}

TString TMirrorDescriber::GetCurrentState() const {
    if (CurrentStateFunc() == &TThis::StateInit) {
        return "StateInitConsumer";
    } else if (CurrentStateFunc() == &TThis::StateWork) {
        return "StateWork";
    }
    return "UNKNOWN";
}

NActors::IActor* CreateMirrorDescriber(
    const ui64 tabletId,
    const NActors::TActorId& readBalancerActorId,
    const TString& topicName,
    const NKikimrPQ::TMirrorPartitionConfig& config
) {
    return new TMirrorDescriber(tabletId, readBalancerActorId, topicName, config);
}


}// NPQ
}// NKikimr
