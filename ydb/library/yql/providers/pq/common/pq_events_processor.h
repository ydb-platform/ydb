#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/executor.h>

#include <atomic>

namespace NYql {

template <typename TEv, ui32 TEventType>
class TTopicEventBase : public NActors::TEventLocal<TEv, TEventType> {
public:
    explicit TTopicEventBase(NYdb::IExecutor::TFunction&& f)
        : Function(std::move(f))
    {}

    void Execute() {
        Function();
    }

private:
    NYdb::IExecutor::TFunction Function;
};

template <typename TTopicEvent>
class TTopicEventProcessor {
    class TEventProxy final : public NYdb::IExecutor {
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
            if (Stopped.load()) {
                // Drop the task immediately - the actor is shutting down.
                // Destroying 'f' here releases any captured shared_ptrs
                // (e.g. TDecompressionTask::Parent), preventing memory leaks
                // from events posted to a dead actor's mailbox.
                return;
            }
            ActorSystem->Send(ExecuterId, new TTopicEvent(std::move(f)));
        }

        void Stop() final {
            Stopped.store(true);
        }

    private:
        void DoStart() final {
        }

    private:
        NActors::TActorSystem* ActorSystem = nullptr;
        const NActors::TActorId ExecuterId;
        std::atomic<bool> Stopped{false};
    };

public:
    ~TTopicEventProcessor() {
        StopExecuterProxy();
    }

    template <typename TSettings>
    void SetupTopicClientSettings(NActors::TActorSystem* actorSystem, const NActors::TActorId& selfId, TSettings& settings) {
        if (!ExecuterProxy) {
            ExecuterProxy = std::make_shared<TEventProxy>(actorSystem, selfId);
        }

        settings.DefaultHandlersExecutor(ExecuterProxy);
        settings.DefaultCompressionExecutor(ExecuterProxy);
    }

protected:
    void HandleTopicEvent(TTopicEvent::TPtr& event) {
        event->Get()->Execute();
    }

    void StopExecuterProxy() {
        if (ExecuterProxy) {
            ExecuterProxy->Stop();
        }
    }

private:
    std::shared_ptr<TEventProxy> ExecuterProxy;
};

} // namespace NYql
