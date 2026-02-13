#pragma once

#include "local_topic_client_settings.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc_operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/events_common.h>

namespace NKikimr::NKqp {

class TLocalTopicClientBase {
public:
    TLocalTopicClientBase(const TLocalTopicClientSettings& localSettings, const NYdb::TCommonClientSettings& clientSettings);

    virtual ~TLocalTopicClientBase() = default;

protected:
    template <typename TRpc, typename TSettings>
    NThreading::TFuture<NRpcService::TLocalRpcOperationResult> DoLocalRpcRequest(typename TRpc::TRequest&& proto, const NYdb::TOperationRequestSettings<TSettings>& settings, NRpcService::TLocalRpcOperationRequestCreator requestCreator) const {
        const auto promise = NThreading::NewPromise<NRpcService::TLocalRpcOperationResult>();
        auto* actor = new NRpcService::TOperationRequestExecuter<TRpc, TSettings>(std::move(proto), {
            .ChannelBufferSize = ChannelBufferSize,
            .OperationSettings = settings,
            .RequestCreator = std::move(requestCreator),
            .Database = Database,
            .Token = CredentialsProvider ? TMaybe<TString>(CredentialsProvider->GetAuthInfo()) : Nothing(),
            .Promise = promise,
            .OperationName = "local_topic_rpc_operation",
        });
        ActorSystem->Register(actor, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
        return promise.GetFuture();
    }

    NActors::TActorSystem* const ActorSystem = nullptr;
    const ui64 ChannelBufferSize = 0;
    const TString Database;
    const std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
};

template <typename TEventVariant>
class TLocalTopicSessionBase {
public:
    struct TEvent {
        TEvent(TEventVariant&& event, i64 size)
            : Event(std::move(event))
            , Size(size)
        {}

        TEventVariant Event;
        i64 Size = 0;
    };

    explicit TLocalTopicSessionBase(const TLocalTopicSessionSettings& localSettings)
        : ActorSystem(localSettings.ActorSystem)
        , Database(localSettings.Database)
        , CredentialsProvider(localSettings.CredentialsProvider)
    {
        Y_VALIDATE(ActorSystem, "Actor system is not set");
        Y_VALIDATE(Database, "Database is not set");
    }

    virtual ~TLocalTopicSessionBase() = default;

protected:
    virtual void RequestEvents(NThreading::TPromise<std::vector<TEvent>> promise) = 0;

    virtual void OnCloseReceived() = 0;

    template <typename TSettings>
    static void ValidateSettings(const NYdb::TRequestSettings<TSettings>& settings) {
        Y_VALIDATE(settings.ClientTimeout_ == TDuration::Max(), "Timeout is not allowed for local topic session");
        Y_VALIDATE(settings.Deadline_ == NYdb::TDeadline::Max(), "Timeout is not allowed for local topic session");
    }

    NThreading::TFuture<void> WaitEvent() {
        if (!Events.empty() || ExtractEvents()) {
            return NThreading::MakeFuture();
        }

        if (!EventFuture) {
            auto eventsPromise = NThreading::NewPromise<std::vector<TEvent>>();
            EventFuture = eventsPromise.GetFuture();
            RequestEvents(std::move(eventsPromise));
        }

        return EventFuture->IgnoreResult();
    }

    bool ExtractEvents() {
        if (!EventFuture || !EventFuture->IsReady()) {
            return false;
        }

        auto events = EventFuture->ExtractValue();
        EventFuture.reset();

        for (auto& event : events) {
            Y_VALIDATE(event.Size >= 0, "Event size must be non negative");
            Events.emplace(std::move(event));

            if (std::holds_alternative<NYdb::NTopic::TSessionClosedEvent>(Events.back().Event)) {
                // Session was finished, skip all other events
                OnCloseReceived();
                break;
            }
        }

        return true;
    }

    NActors::TActorSystem* const ActorSystem = nullptr;
    const TString Database;
    const std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
    std::optional<NThreading::TFuture<std::vector<TEvent>>> EventFuture;
    std::queue<TEvent> Events;
};

} // namespace NKikimr::NKqp
