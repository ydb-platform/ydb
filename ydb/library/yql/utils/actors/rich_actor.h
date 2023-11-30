#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NYql {

template<typename TDerived>
class TRichActor: public NActors::TActor<TDerived> {
public:
    TRichActor(void (TDerived::*func)(TAutoPtr<NActors::IEventHandle>& ev))
        : NActors::TActor<TDerived>(func)
    { }

    ~TRichActor() {
        ForgetChildren();
    }

    virtual void DoPassAway() { }

    void PassAway() final {
        DoPassAway();
        CleanupChildren();
        UnsubscribeAll();
        NActors::IActor::PassAway();
        Killed = true;
    }

    void CleanupChildren() {
        for (auto [id, killEvent] : Children) {
            NActors::TActor<TDerived>::Send(id,
                killEvent
                    ? killEvent
                    : new NActors::TEvents::TEvPoison());
        }
        Children.clear();
    }

    void ForgetChildren() { // Free memory
        for (auto&& [child, killEvent] : Children) {
            delete killEvent;
        }
        Children.clear();
    }

    void UnsubscribeAll() {
        auto copy = Subscriptions;
        for (auto id : copy) {
            Unsubscribe(id);
        }
    }

    void Unsubscribe(ui32 nodeId) {
        if (nodeId != NActors::TActor<TDerived>::SelfId().NodeId()) {
            NActors::TActor<TDerived>::Send(NActors::TActivationContext::InterconnectProxy(nodeId), new NActors::TEvents::TEvUnsubscribe());
        }
        Subscriptions.erase(nodeId);
    }

    NActors::TActorId RegisterChild(NActors::IActor* actor, NActors::IEventBase* killEvent = nullptr, ui32 poolId = Max<ui32>()) {
        auto id = NActors::TActor<TDerived>::Register(actor, NActors::TMailboxType::HTSwap, poolId);
        Children.insert(std::make_pair(id, killEvent));
        return id;
    }

    NActors::TActorId RegisterLocalChild(NActors::IActor* actor, NActors::IEventBase* killEvent = nullptr) {
        auto id = NActors::TActor<TDerived>::RegisterWithSameMailbox(actor);
        Children.insert(std::make_pair(id, killEvent));
        return id;
    }

    void UnregisterChild(NActors::TActorId id) {
        auto it = Children.find(id);
        if (it != Children.end()) {
            NActors::TActor<TDerived>::Send(id,
                it->second
                    ? it->second
                    : new NActors::TEvents::TEvPoison());
            Children.erase(id);
        }
    }

    void AddChild(NActors::TActorId id, NActors::IEventBase* killEvent = nullptr) {
        Children.insert(std::make_pair(id, killEvent));
    }

    void Subscribe(ui32 nodeId) {
        Subscriptions.insert(nodeId);
    }

protected:
    bool Killed = false;

private:
    THashMap<NActors::TActorId, NActors::IEventBase*> Children;
    THashSet<ui32> Subscriptions;
};

} // namespace NYql
