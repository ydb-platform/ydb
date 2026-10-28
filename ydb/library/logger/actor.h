#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>

#include <atomic>
#include <memory>
#include <mutex>


namespace NKikimr {

// Log backend that allows us to create shared YDB driver early (before actor system starts),
// but log to actor system.
//
// store(nullptr) and WriteData share one mutex. WriteData holds it for the whole
// LOG_LOG, so a shutdown that clears the pointer waits out in-flight logs and later
// logs no longer touch the actor system.
class TDeferredActorLogBackend : public TLogBackend {
public:
    class TAtomicActorSystemPtr {
    public:
        explicit TAtomicActorSystemPtr(NActors::TActorSystem* actorSystem = nullptr)
            : ActorSystem(actorSystem)
        {
        }

        TAtomicActorSystemPtr(const TAtomicActorSystemPtr&) = delete;
        TAtomicActorSystemPtr& operator=(const TAtomicActorSystemPtr&) = delete;

        void store(NActors::TActorSystem* actorSystem, std::memory_order /*order*/ = std::memory_order_seq_cst) {
            std::lock_guard guard(Lock);
            ActorSystem = actorSystem;
        }

        NActors::TActorSystem* load(std::memory_order /*order*/ = std::memory_order_seq_cst) const {
            std::lock_guard guard(Lock);
            return ActorSystem;
        }

    private:
        friend class TDeferredActorLogBackend;

        template <typename TFunc>
        void WithActorSystem(TFunc&& func) const {
            std::lock_guard guard(Lock);
            func(ActorSystem);
        }

        mutable std::mutex Lock;
        NActors::TActorSystem* ActorSystem = nullptr;
    };

    using TSharedAtomicActorSystemPtr = std::shared_ptr<TAtomicActorSystemPtr>;

    TDeferredActorLogBackend(TSharedAtomicActorSystemPtr actorSystem, int logComponent);

    NActors::NLog::EPriority GetActorLogPriority(ELogPriority priority) const;

    void WriteData(const TLogRecord& rec) override;

    void ReopenLog() override {}

protected:
    TSharedAtomicActorSystemPtr ActorSystemPtr;
    const int LogComponent;
};

} // NKikimr

