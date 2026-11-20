#include "distconf.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

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
            auto ev = std::make_unique<TEvNodeWardenDynamicConfigSubscribe>();
            ev->Record.SetBridgeCapable(static_cast<bool>(Cfg->BridgeConfig));
            ev->Record.SetPileListChangeCapable(true);
            ev->Record.SetBridgeFeatureLevel(BridgeFeatureLevel);
            TActivationContext::Send(new IEventHandle(MakeBlobStorageNodeWardenID(ConnectedToStaticNode), SelfId(),
                ev.release(), IEventHandle::FlagSubscribeOnSession, ++StaticNodeSubscriptionCookie));
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

    void TDistributedConfigKeeper::OnStaticNodeConnected(ui32 nodeId, TActorId sessionId, ui64 cookie) {
        if (cookie == StaticNodeSubscriptionCookie) {
            Y_ABORT_UNLESS(nodeId == ConnectedToStaticNode);
            Y_ABORT_UNLESS(!StaticNodeSessionId);
            StaticNodeSessionId = sessionId;
        }
    }

    void TDistributedConfigKeeper::OnStaticNodeDisconnected(ui32 nodeId, TActorId sessionId, ui64 cookie) {
        if (nodeId != ConnectedToStaticNode || (StaticNodeSessionId && sessionId != StaticNodeSessionId) || cookie != StaticNodeSubscriptionCookie) {
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
        if (record.HasCacheUpdate()) {
            ApplyCacheUpdates(record.MutableCacheUpdate(), 0);
        }
    }

    void TDistributedConfigKeeper::ApplyConfigUpdateToDynamicNodes(bool drop) {
        for (const auto& [sessionId, actorId] : DynamicConfigSubscribers) {
            const auto it = DynamicNodeFeatureLevels.find(actorId.NodeId());
            PushConfigToDynamicNode(actorId, sessionId, false, it != DynamicNodeFeatureLevels.end() ? it->second : 0);
        }
        if (drop) {
            Y_ABORT_UNLESS(!PartOfNodeQuorum());
            DynamicConfigSubscribers.clear();
            UnsubscribeQueue.insert(ConnectedDynamicNodes.begin(), ConnectedDynamicNodes.end());
            ConnectedDynamicNodes.clear();
            BridgeCapableDynamicNodes.clear();
            DynamicNodeFeatureLevels.clear();
        }
    }

    void TDistributedConfigKeeper::OnDynamicNodeDisconnected(ui32 nodeId, TActorId sessionId) {
        ConnectedDynamicNodes.erase(nodeId);
        BridgeCapableDynamicNodes.erase(nodeId);
        DynamicNodeFeatureLevels.erase(nodeId);
        DynamicConfigSubscribers.erase(sessionId);
        UnsubscribeQueue.insert(nodeId);
    }

    void TDistributedConfigKeeper::Handle(TEvNodeWardenDynamicConfigSubscribe::TPtr ev) {
        const TActorId sessionId = ev->InterconnectSession;
        Y_ABORT_UNLESS(sessionId);

        const bool partOfNodeQuorum = PartOfNodeQuorum();
        const auto& subscription = ev->Get()->Record;
        const ui32 bridgeFeatureLevel = GetReportedBridgeFeatureLevel(subscription.GetBridgeFeatureLevel(),
            subscription.GetPileListChangeCapable());
        if (!partOfNodeQuorum || StorageConfig) {
            PushConfigToDynamicNode(ev->Sender, sessionId, true, bridgeFeatureLevel);
        }
        if (!partOfNodeQuorum) {
            return;
        }

        const ui32 peerNodeId = ev->Sender.NodeId();
        SubscribeToPeerNode(peerNodeId, sessionId);
        ConnectedDynamicNodes.insert(peerNodeId);
        if (ev->Get()->Record.GetBridgeCapable()) { // older nodes do not report anything, they are not bridge-capable
            BridgeCapableDynamicNodes.insert(peerNodeId);
        } else {
            BridgeCapableDynamicNodes.erase(peerNodeId);
        }
        if (bridgeFeatureLevel) {
            DynamicNodeFeatureLevels[peerNodeId] = bridgeFeatureLevel;
        } else {
            DynamicNodeFeatureLevels.erase(peerNodeId);
        }
        const auto [_, inserted] = DynamicConfigSubscribers.try_emplace(sessionId, ev->Sender);
        Y_ABORT_UNLESS(inserted);
    }

    void TDistributedConfigKeeper::PushConfigToDynamicNode(TActorId actorId, TActorId sessionId, bool addCache,
            ui32 bridgeFeatureLevel) {
        auto ev = std::make_unique<TEvNodeWardenDynamicConfigPush>();
        auto& record = ev->Record;
        const auto& config = SelfManagementEnabled ? StorageConfig : BaseConfig;
        if (!PartOfNodeQuorum()) { // this configuration is not reliable, don't push anything
            ev->Record.SetNoQuorum(true);
        } else if (!StorageConfig) {
            // no storage configuration -- no nothing
        } else if (const ui32 required = GetRequiredBridgeFeatureLevel(*config); bridgeFeatureLevel < required) {
            // the node can't work with this configuration, it has to be upgraded
            YDB_LOG_WARN("Dynamic node runs version not supporting bridge features of the config, config is not pushed",
                {"marker", "NWDC88"},
                {"nodeId", actorId.NodeId()},
                {"bridgeFeatureLevel", bridgeFeatureLevel},
                {"requiredBridgeFeatureLevel", required});
        } else {
            record.MutableConfig()->CopyFrom(*config);
        }
        if (addCache) {
            // push full cache to the dynamic node
            for (auto it = Cache.begin(); it != Cache.end(); ++it) {
                AddCacheUpdate(record.MutableCacheUpdate(), it, true);
            }
        }
        auto handle = std::make_unique<IEventHandle>(actorId, SelfId(), ev.release());
        handle->Rewrite(TEvInterconnect::EvForward, sessionId);
        TActivationContext::Send(handle.release());
    }

} // NKikimr::NStorage
