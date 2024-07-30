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

    void TDistributedConfigKeeper::OnStaticNodeDisconnected(ui32 nodeId, TActorId sessionId) {
        if (nodeId != ConnectedToStaticNode || (StaticNodeSessionId && StaticNodeSessionId != sessionId)) {
            return; // possible race with unsubscription
        }
        ConnectedToStaticNode = 0;
        StaticNodeSessionId = {};
        ConnectToStaticNode();
    }

    void TDistributedConfigKeeper::Handle(TEvNodeWardenDynamicConfigPush::TPtr ev) {
        if (ev->Sender.NodeId() != ConnectedToStaticNode || !StaticNodeSessionId || ev->InterconnectSession != StaticNodeSessionId) {
            return; // this may be a race with unsubscription
        }
        auto& record = ev->Get()->Record;
        if (record.HasConfig()) {
            ApplyStorageConfig(record.GetConfig());
        }
        if (record.GetNoQuorum()) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, StaticNodeSessionId, SelfId(),
                nullptr, 0));
            ConnectedToStaticNode = 0;
            StaticNodeSessionId = {};
            ConnectToStaticNode();
        }
    }

    void TDistributedConfigKeeper::ApplyConfigUpdateToDynamicNodes(bool drop) {
        for (const auto& [sessionId, actorId] : DynamicConfigSubscribers) {
            PushConfigToDynamicNode(actorId, sessionId);
        }
        if (drop) {
            Y_ABORT_UNLESS(!PartOfNodeQuorum());
            DynamicConfigSubscribers.clear();
            UnsubscribeQueue.insert(ConnectedDynamicNodes.begin(), ConnectedDynamicNodes.end());
            ConnectedDynamicNodes.clear();
        }
    }

    void TDistributedConfigKeeper::OnDynamicNodeDisconnected(ui32 nodeId, TActorId sessionId) {
        ConnectedDynamicNodes.erase(nodeId);
        DynamicConfigSubscribers.erase(sessionId);
        UnsubscribeQueue.insert(nodeId);
    }

    void TDistributedConfigKeeper::HandleDynamicConfigSubscribe(STATEFN_SIG) {
        const TActorId sessionId = ev->InterconnectSession;
        Y_ABORT_UNLESS(sessionId);

        const bool partOfNodeQuorum = PartOfNodeQuorum();
        if (!partOfNodeQuorum || StorageConfig) {
            PushConfigToDynamicNode(ev->Sender, sessionId);
        }
        if (!partOfNodeQuorum) {
            return;
        }

        const ui32 peerNodeId = ev->Sender.NodeId();
        SubscribeToPeerNode(peerNodeId, sessionId);
        ConnectedDynamicNodes.insert(peerNodeId);
        const auto [_, inserted] = DynamicConfigSubscribers.try_emplace(sessionId, ev->Sender);
        Y_ABORT_UNLESS(inserted);
    }

    void TDistributedConfigKeeper::PushConfigToDynamicNode(TActorId actorId, TActorId sessionId) {
        auto ev = std::make_unique<TEvNodeWardenDynamicConfigPush>();
        auto& record = ev->Record;
        if (StorageConfig) {
            record.MutableConfig()->CopyFrom(*StorageConfig);
        }
        if (!PartOfNodeQuorum()) {
            ev->Record.SetNoQuorum(true);
        }
        auto handle = std::make_unique<IEventHandle>(actorId, SelfId(), ev.release());
        handle->Rewrite(TEvInterconnect::EvForward, sessionId);
        TActivationContext::Send(handle.release());
    }

} // NKikimr::NStorage
