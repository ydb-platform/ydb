#pragma once

#include <ydb/library/actors/core/actorsystem.h>

#include <mutex>

namespace NYql::NNative {

// Actor cleanup detaches the destination before TActorSystem is destroyed.
// External callbacks may retain this object without retaining either actor.
class TCallbackMailbox {
public:
    TCallbackMailbox(NActors::TActorSystem* system, NActors::TActorId actor)
        : System_(system), Actor_(actor) {}

    void Send(NActors::IEventBase* event) {
        {
            std::lock_guard lock(Mutex_);
            if (System_) {
                System_->Send(Actor_, event);
                return;
            }
        }
        // Destroy outside the lock: an event may own the last attempt lease,
        // whose destructor itself sends a final notification to this mailbox.
        delete event;
    }

    void Detach() {
        std::lock_guard lock(Mutex_);
        System_ = nullptr;
    }

private:
    // Send may synchronously destroy an undeliverable event. Its last lease can
    // re-enter Send to report quiescence while we still guard System_.
    std::recursive_mutex Mutex_;
    NActors::TActorSystem* System_;
    const NActors::TActorId Actor_;
};

} // namespace NYql::NNative
