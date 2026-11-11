#include "mirror_describer.h"

#include <ydb/core/persqueue/common/proxy/mirror_credentials.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <google/protobuf/util/message_differencer.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

#define PQ_ENSURE(condition) AFL_ENSURE(condition)("topic", TopicName)

using namespace NPersQueue;

namespace NKikimr {
namespace NPQ {

TMirrorDescriber::TMirrorDescriber(
    ui64 tabletId,
    TActorId readBalancerActorId,
    TString topicName,
    const NKikimrPQ::TMirrorPartitionConfig& config,
    const TString& database,
    const TString& topicPath
)
    : TBaseTabletActor(tabletId, readBalancerActorId, NKikimrServices::PQ_MIRROR_DESCRIBER)
    , TopicName(std::move(topicName))
    , Config(config)
    , Database(database)
    , TopicPath(topicPath)
{
}

void TMirrorDescriber::Bootstrap(const TActorContext& ctx) {
    StartInit(ctx);
}

void TMirrorDescriber::StartInit(const TActorContext& ctx) {
    Become(&TThis::StateInit);
    DescribeRetryTimeout = DESCRIBE_RETRY_TIMEOUT_START;
    ++DescribeGeneration;
    DescribeTopicRequestInFlight = false;
    CredentialsRequestInFlight = false;
    if (auto initializer = std::exchange(CredentialsInitializer, {})) {
        ctx.Send(initializer, new TEvents::TEvPoisonPill());
    }
    ctx.Send(SelfId(), new TEvPQ::TEvInitCredentials, 0, DescribeGeneration);
}

void TMirrorDescriber::Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    LOG_N("Killed");
    if (auto initializer = std::exchange(CredentialsInitializer, {})) {
        ctx.Send(initializer, new TEvents::TEvPoisonPill());
    }
    CredentialsProvider = nullptr;
    Die(ctx);
}

void TMirrorDescriber::HandleChangeConfig(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx) {
    const auto& tabletConfig = ev->Get()->Config;
    bool equalConfigs = google::protobuf::util::MessageDifferencer::Equals(
        Config,
        tabletConfig.GetPartitionConfig().GetMirrorFrom()
    );
    const bool equalContext = Database == tabletConfig.GetYdbDatabasePath() && TopicPath == tabletConfig.GetTopicPath();
    LOG_D(
        "Got new config, equal with",
        {"previous", equalConfigs && equalContext}
    );
    if (!equalConfigs || !equalContext) {
        Config = tabletConfig.GetPartitionConfig().GetMirrorFrom();
        Database = tabletConfig.GetYdbDatabasePath();
        TopicPath = tabletConfig.GetTopicPath();
        LOG_I("Changing config");
        StartInit(ctx);
    }
}

void TMirrorDescriber::HandleDescriptionResult(TEvPQ::TEvMirrorTopicDescription::TPtr& ev, const TActorContext& ctx) {
    if (ev->Cookie != DescribeGeneration) {
        LOG_D(
            "Ignoring stale topic description",
            {"cookie", ev->Cookie},
                    {"generation",
            DescribeGeneration}
        );
        return;
    }
    DescribeTopicRequestInFlight = false;
    const auto& description = ev->Get()->Description;
    if (!description.has_value()) {
        LOG_E(
            "Cannot describe topic",
            {"error", description.error()}
        );
        ScheduleWithIncreasingTimeout<TEvents::TEvWakeup>(SelfId(), DescribeRetryTimeout, DESCRIBE_RETRY_TIMEOUT_MAX, ctx);
        return;
    }
    const NYdb::NTopic::TDescribeTopicResult& result = description.value();
    if (!result.IsSuccess()) {
        LOG_E(
            "Cannot describe topic",
            {"issues", result.GetIssues()}
        );
        ScheduleWithIncreasingTimeout<TEvents::TEvWakeup>(SelfId(), DescribeRetryTimeout, DESCRIBE_RETRY_TIMEOUT_MAX, ctx);
        return;
    }
    auto debugTopicDescriptionString = [](const NYdb::NTopic::TTopicDescription& descr) {
        Ydb::Topic::CreateTopicRequest req;
        descr.SerializeTo(req);
        return req.ShortUtf8DebugString();
    };
    LOG_T(
        "Topic",
        {"description", debugTopicDescriptionString(description.value().GetTopicDescription())}
    );
    ctx.Send(TabletActorId, ev->Release());
    ctx.Schedule(DESCRIBE_RETRY_TIMEOUT_MAX, new TEvents::TEvWakeup());
}

void TMirrorDescriber::DescribeTopic(const TActorContext& ctx) {
    if (DescribeTopicRequestInFlight) {
        LOG_I("Description request already inflight");
        return;
    }

    auto factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    PQ_ENSURE(factory);
    auto future = factory->GetTopicDescription(Config, CredentialsProvider);
    future.Subscribe(
        [
            actorSystem = ctx.ActorSystem(),
            selfId = SelfId(),
            generation = DescribeGeneration
        ](const NThreading::TFuture<NYdb::NTopic::TDescribeTopicResult>& result) {
            THolder<TEvPQ::TEvMirrorTopicDescription> ev;
            const bool hasValue = result.HasValue();
            if (hasValue) {
                const auto& value = result.GetValue();
                ev = MakeHolder<TEvPQ::TEvMirrorTopicDescription>(value);
                actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev.Release(), 0, generation));
                return;
            }
            try {
                ev = MakeHolder<TEvPQ::TEvMirrorTopicDescription>(result.GetValue());
            } catch (...) {
                ev = MakeHolder<TEvPQ::TEvMirrorTopicDescription>(CurrentExceptionMessage());
            }
            actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev.Release(), 0, generation));
        }
    );
    DescribeTopicRequestInFlight = true;
}

void TMirrorDescriber::HandleInitCredentials(TEvPQ::TEvInitCredentials::TPtr& ev, const TActorContext& ctx) {
    if (ev->Cookie != 0 && ev->Cookie != DescribeGeneration) {
        LOG_D(
            "Ignoring stale credentials init",
            {"cookie", ev->Cookie},
                    {"generation",
            DescribeGeneration}
        );
        return;
    }
    if (CredentialsRequestInFlight) {
        LOG_W("Credentials request already inflight");
        return;
    }
    CredentialsProvider = nullptr;

    auto factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    PQ_ENSURE(factory);
    if (MirrorCredentialsNeedResolution(Config.GetCredentials())) {
        CredentialsInitializer = ctx.Register(CreateMirrorCredentialsInitializer(
            SelfId(), DescribeGeneration, Config.GetCredentials(), Database, TopicPath, factory
        ));
        CredentialsRequestInFlight = true;
        return;
    }

    auto future = factory->GetCredentialsProvider(Config.GetCredentials());
    future.Subscribe(
        [
            actorSystem = ctx.ActorSystem(),
            selfId = SelfId(),
            generation = DescribeGeneration
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
            actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev.Release(), 0, generation));
        }
    );
    CredentialsRequestInFlight = true;
}

void TMirrorDescriber::HandleCredentialsCreated(TEvPQ::TEvCredentialsCreated::TPtr& ev, const TActorContext& ctx) {
    if (ev->Cookie != DescribeGeneration) {
        LOG_D(
            "Ignoring stale credentials",
            {"cookie", ev->Cookie},
                    {"generation",
            DescribeGeneration}
        );
        return;
    }
    CredentialsInitializer = {};
    CredentialsRequestInFlight = false;
    if (ev->Get()->Error) {
        LOG_W(
            "Cannot initialize credentials",
            {"provider", ev->Get()->Error.value()}
        );
        ctx.Schedule(CredentialsInitInterval, std::make_unique<IEventHandle>(
            SelfId(), SelfId(), new TEvPQ::TEvInitCredentials(), 0, DescribeGeneration
        ));
        CredentialsInitInterval = Min(CredentialsInitInterval * 2, INIT_INTERVAL_MAX);
        return;
    }

    CredentialsProvider = ev->Get()->Credentials;
    LOG_N(
        "Credentials provider created",
        {"hasCredentialsProvider", bool(CredentialsProvider)}
    );
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

TStructuredMessage TMirrorDescriber::BuildLogPrefix() const {
    return YDB_LOG_CREATE_MESSAGE(
        {"topic", TopicName});
}

NActors::IActor* CreateMirrorDescriber(
    const ui64 tabletId,
    const NActors::TActorId& readBalancerActorId,
    const TString& topicName,
    const NKikimrPQ::TMirrorPartitionConfig& config,
    const TString& database,
    const TString& topicPath
) {
    return new TMirrorDescriber(tabletId, readBalancerActorId, topicName, config, database, topicPath);
}


}// NPQ
}// NKikimr
