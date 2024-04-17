#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/interconnect.h>

namespace NActors::NInterconnect {

    class TSubscriptionManager {
        TActorIdentity SelfId;
        THashMap<ui32, TActorId> Nodes;

    public:
        ~TSubscriptionManager() {
            for (const auto& [nodeId, actorId] : Nodes) {
                Y_ABORT_UNLESS(SelfId);
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, actorId, SelfId, nullptr, 0));
            }
        }

        // SetSelfId is called on bootstrap of the owning actor.
        void SetSelfId(TActorIdentity selfId) {
            SelfId = selfId;
        }

        // Arm is called at the time of sending FlagSubscribeOnSession/TEvConnectNode/TEvSubscribe event to proxy or session,
        // where the 'target' parameter points to selected actor (either InterconnectProxy or existing Session actor).
        void Arm(ui32 nodeId, TActorId target) {
            const auto [it, inserted] = Nodes.emplace(nodeId, target);
            Y_ABORT_UNLESS(inserted);
        }

        // Unsubscribe can be invoked to unsubscribe immediately. IsSubscribed(nodeId) MUST BE true for this to work.
        void Unsubscribe(ui32 nodeId) {
            const auto it = Nodes.find(nodeId);
            Y_ABORT_UNLESS(it != Nodes.end());
            Y_ABORT_UNLESS(SelfId);
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, it->second, SelfId, nullptr, 0));
            Nodes.erase(it);
        }

        // IsSubscribed is used to determine if the TEvNodeConnected/TEvNodeDisconnected is expected from the session.
        bool IsSubscribed(ui32 nodeId) const {
            return Nodes.contains(nodeId);
        }

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
            if (const auto it = Nodes.find(ev->Get()->NodeId); it != Nodes.end()) {
                it->second = ev->Sender;
            }
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
            Nodes.erase(ev->Get()->NodeId);
        }
    };

} // NActors::NInterconnect
