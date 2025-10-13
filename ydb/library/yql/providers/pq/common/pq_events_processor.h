#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/executor.h>

namespace NYql {

template <typename TEv, ui32 TEventType>
class TTopicEventBase : public NActors::TEventLocal<TEv, TEventType> {
public:
    explicit TTopicEventBase(NYdb::NTopic::IExecutor::TFunction&& f)
        : Function(std::move(f))
    {}

    void Execute() {
        Function();
    }

private:
    NYdb::NTopic::IExecutor::TFunction Function;
};

template <typename TTopicEvent>
class TTopicEventProcessor {
    class TEventProxy final : public NYdb::NTopic::IExecutor {
    public:
        TEventProxy(NActors::TActorSystem* actorSystem, const NActors::TActorId& executerId)
            : ActorSystem(actorSystem)
            , ExecuterId(executerId)
        {
            Y_ENSURE(actorSystem);
        }

        bool IsAsync() const final {
            return true;
        }

        void Post(TFunction&& f) final {
            ActorSystem->Send(ExecuterId, new TTopicEvent(std::move(f)));
        }

    private:
        void DoStart() final {
        }

    private:
        NActors::TActorSystem* ActorSystem = nullptr;
        const NActors::TActorId ExecuterId;
    };

public:
    template <typename TSettings>
    void SetupTopicClientSettings(NActors::TActorSystem* actorSystem, const NActors::TActorId& selfId, TSettings& settings) {
        if (!ExecuterProxy) {
            ExecuterProxy = MakeIntrusive<TEventProxy>(actorSystem, selfId);
        }

        settings.DefaultHandlersExecutor(ExecuterProxy);
        settings.DefaultCompressionExecutor(ExecuterProxy);
    }

protected:
    void HandleTopicEvent(TTopicEvent::TPtr& event) {
        event->Get()->Execute();
    }

private:
    NYdb::NTopic::IExecutor::TPtr ExecuterProxy;
};

} // namespace NYql
