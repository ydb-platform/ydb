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
<<<<<<< HEAD
    ctx.Send(SelfId(), new TEvPQ::TEvInitCredentials);
}

void TMirrorDescriber::Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    YDB_LOG_NOTICE("Killed",
        {"logPrefix", NPQ_LOG_PREFIX});
=======
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
>>>>>>> 155ca05c7e3 ([KIKIMR-27673] Support secret references in topic mirroring credentials (#59067))
    CredentialsProvider = nullptr;
    Die(ctx);
}

void TMirrorDescriber::HandleChangeConfig(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx) {
    const auto& tabletConfig = ev->Get()->Config;
    bool equalConfigs = google::protobuf::util::MessageDifferencer::Equals(
        Config,
        tabletConfig.GetPartitionConfig().GetMirrorFrom()
    );
<<<<<<< HEAD
    YDB_LOG_DEBUG("Got new config, equal with",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"previous", equalConfigs});
    if (!equalConfigs) {
        Config = ev->Get()->Config.GetPartitionConfig().GetMirrorFrom();
        YDB_LOG_INFO("Changing config",
            {"logPrefix", NPQ_LOG_PREFIX});
=======
    const bool equalPathsContext = Database == tabletConfig.GetYdbDatabasePath() && TopicPath == tabletConfig.GetTopicPath();
    LOG_D(
        "Got new config, equal with",
        {"previous", equalConfigs && equalPathsContext}
    );
    if (!equalConfigs || !equalPathsContext) {
        Config = tabletConfig.GetPartitionConfig().GetMirrorFrom();
        Database = tabletConfig.GetYdbDatabasePath();
        TopicPath = tabletConfig.GetTopicPath();
        LOG_I("Changing config");
>>>>>>> 155ca05c7e3 ([KIKIMR-27673] Support secret references in topic mirroring credentials (#59067))
        StartInit(ctx);
    }
}

void TMirrorDescriber::HandleDescriptionResult(TEvPQ::TEvMirrorTopicDescription::TPtr& ev, const TActorContext& ctx) {
    DescribeTopicRequestInFlight = false;
    const auto& description = ev->Get()->Description;
    if (!description.has_value()) {
        YDB_LOG_ERROR("Cannot describe topic",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"error", description.error()});
        ScheduleWithIncreasingTimeout<TEvents::TEvWakeup>(SelfId(), DescribeRetryTimeout, DESCRIBE_RETRY_TIMEOUT_MAX, ctx);
        return;
    }
    const NYdb::NTopic::TDescribeTopicResult& result = description.value();
    if (!result.IsSuccess()) {
        YDB_LOG_ERROR("Cannot describe topic",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"issues", result.GetIssues()});
        ScheduleWithIncreasingTimeout<TEvents::TEvWakeup>(SelfId(), DescribeRetryTimeout, DESCRIBE_RETRY_TIMEOUT_MAX, ctx);
        return;
    }
    auto debugTopicDescriptionString = [](const NYdb::NTopic::TTopicDescription& descr) {
        Ydb::Topic::CreateTopicRequest req;
        descr.SerializeTo(req);
        return req.ShortUtf8DebugString();
    };
    YDB_LOG_TRACE("Topic",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"description", debugTopicDescriptionString(description.value().GetTopicDescription())});
    ctx.Send(TabletActorId, ev->Release());
    ctx.Schedule(DESCRIBE_RETRY_TIMEOUT_MAX, new TEvents::TEvWakeup());
}

void TMirrorDescriber::DescribeTopic(const TActorContext& ctx) {
    if (DescribeTopicRequestInFlight) {
        YDB_LOG_INFO("Description request already inflight",
            {"logPrefix", NPQ_LOG_PREFIX});
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
        YDB_LOG_WARN("Credentials request already inflight",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }
    CredentialsProvider = nullptr;

    auto factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    PQ_ENSURE(factory);
<<<<<<< HEAD
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
=======
    CredentialsInitializer = ctx.Register(CreateMirrorCredentialsInitializer(
        SelfId(), DescribeGeneration, Config.GetCredentials(), Database, TopicPath, factory
    ));
>>>>>>> 155ca05c7e3 ([KIKIMR-27673] Support secret references in topic mirroring credentials (#59067))
    CredentialsRequestInFlight = true;
}

void TMirrorDescriber::HandleCredentialsCreated(TEvPQ::TEvCredentialsCreated::TPtr& ev, const TActorContext& ctx) {
<<<<<<< HEAD
    CredentialsRequestInFlight = false;
    if (ev->Get()->Error) {
        YDB_LOG_WARN("Cannot initialize credentials",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"provider", ev->Get()->Error.value()});
        ScheduleWithIncreasingTimeout<TEvPQ::TEvInitCredentials>(SelfId(), CredentialsInitInterval, INIT_INTERVAL_MAX, ctx);
=======
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
>>>>>>> 155ca05c7e3 ([KIKIMR-27673] Support secret references in topic mirroring credentials (#59067))
        return;
    }

    CredentialsProvider = ev->Get()->Credentials;
    YDB_LOG_NOTICE("Credentials provider created",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"hasCredentialsProvider", bool(CredentialsProvider)});
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
    const NKikimrPQ::TMirrorPartitionConfig& config,
    const TString& database,
    const TString& topicPath
) {
    return new TMirrorDescriber(tabletId, readBalancerActorId, topicName, config, database, topicPath);
}


}// NPQ
}// NKikimr
