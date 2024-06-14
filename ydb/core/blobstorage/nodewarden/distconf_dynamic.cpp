#include "distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::ApplyStaticNodeIds(const std::vector<ui32>& nodeIds) {
        StaticBindQueue.Update(nodeIds);
        ConnectToStaticNode();
    }

    void TDistributedConfigKeeper::ConnectToStaticNode() {
        if (ConnectedToStaticNode) {
            return;
        }

        const TMonotonic now = TActivationContext::Monotonic();
        TMonotonic timestamp;
        if (std::optional<ui32> nodeId = StaticBindQueue.Pick(now, &timestamp)) {
            ConnectedToStaticNode = *nodeId;
            TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvNodeWardenDynamicConfigSubscribe,
                IEventHandle::FlagSubscribeOnSession, MakeBlobStorageNodeWardenID(ConnectedToStaticNode), SelfId(),
                nullptr, 0));
        } else if (timestamp != TMonotonic::Max()) {
            if (!ReconnectScheduled) {
                TActivationContext::Schedule(timestamp, new IEventHandle(TEvPrivate::EvReconnect, 0, SelfId(), {}, nullptr, 0));
                ReconnectScheduled = true;
            }
        }
    }

    void TDistributedConfigKeeper::HandleReconnect() {
        Y_ABORT_UNLESS(ReconnectScheduled);
        ReconnectScheduled = false;
        ConnectToStaticNode();
    }

    void TDistributedConfigKeeper::OnStaticNodeConnected(ui32 nodeId, TActorId sessionId) {
        Y_ABORT_UNLESS(nodeId == ConnectedToStaticNode);
        Y_ABORT_UNLESS(!StaticNodeSessionId);
        StaticNodeSessionId = sessionId;
    }

    void TDistributedConfigKeeper::OnStaticNodeDisconnected(ui32 nodeId, TActorId /*sessionId*/) {
        Y_ABORT_UNLESS(nodeId == ConnectedToStaticNode);
        ConnectedToStaticNode = 0;
        StaticNodeSessionId = {};
        ConnectToStaticNode();
    }

    void TDistributedConfigKeeper::Handle(TEvNodeWardenDynamicConfigPush::TPtr ev) {
        auto& record = ev->Get()->Record;
        ApplyStorageConfig(record.GetConfig());
    }

    void TDistributedConfigKeeper::ApplyConfigUpdateToDynamicNodes() {
        for (const auto& [sessionId, actorId] : DynamicConfigSubscribers) {
            PushConfigToDynamicNode(actorId, sessionId);
        }
    }

    void TDistributedConfigKeeper::OnDynamicNodeDisconnected(ui32 nodeId, TActorId sessionId) {
        ConnectedDynamicNodes.erase(nodeId);
        DynamicConfigSubscribers.erase(sessionId);
    }

    void TDistributedConfigKeeper::HandleDynamicConfigSubscribe(STATEFN_SIG) {
        const TActorId sessionId = ev->InterconnectSession;
        Y_ABORT_UNLESS(sessionId);
        const ui32 peerNodeId = ev->Sender.NodeId();
        if (const auto [it, inserted] = SubscribedSessions.try_emplace(peerNodeId); inserted) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Subscribe, IEventHandle::FlagTrackDelivery,
                sessionId, SelfId(), nullptr, 0));
        }
        ConnectedDynamicNodes.insert(peerNodeId);
        const auto [_, inserted] = DynamicConfigSubscribers.try_emplace(sessionId, ev->Sender);
        Y_ABORT_UNLESS(inserted);

        if (StorageConfig) {
            PushConfigToDynamicNode(ev->Sender, sessionId);
        }
    }

    void TDistributedConfigKeeper::PushConfigToDynamicNode(TActorId actorId, TActorId sessionId) {
        auto ev = std::make_unique<TEvNodeWardenDynamicConfigPush>();
        ev->Record.MutableConfig()->CopyFrom(*StorageConfig);
        auto handle = std::make_unique<IEventHandle>(actorId, SelfId(), ev.release());
        handle->Rewrite(TEvInterconnect::EvForward, sessionId);
        TActivationContext::Send(handle.release());
    }

} // NKikimr::NStorage
