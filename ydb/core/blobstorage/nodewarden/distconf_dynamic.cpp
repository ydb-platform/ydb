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
        for (const auto& [sessionId, subscriber] : DynamicConfigSubscribers) {
            PushConfigToDynamicNode(subscriber, sessionId, false);
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

    void TDistributedConfigKeeper::Handle(TEvNodeWardenDynamicConfigSubscribe::TPtr ev) {
        const TActorId sessionId = ev->InterconnectSession;
        Y_ABORT_UNLESS(sessionId);

        const bool partOfNodeQuorum = PartOfNodeQuorum();
        const auto& subscription = ev->Get()->Record;
        const TDynamicConfigSubscriber subscriber{
            .ActorId = ev->Sender,
            .BridgeCapable = subscription.GetBridgeCapable(),
            .BridgeFeatureLevel = GetReportedBridgeFeatureLevel(subscription.GetBridgeFeatureLevel(),
                subscription.GetPileListChangeCapable()),
        };
        if (!partOfNodeQuorum || StorageConfig) {
            PushConfigToDynamicNode(subscriber, sessionId, true);
        }
        if (!partOfNodeQuorum) {
            return;
        }

        const ui32 peerNodeId = ev->Sender.NodeId();
        SubscribeToPeerNode(peerNodeId, sessionId);
        ConnectedDynamicNodes.insert(peerNodeId);
        const auto [_, inserted] = DynamicConfigSubscribers.try_emplace(sessionId, subscriber);
        Y_ABORT_UNLESS(inserted);
    }

    void TDistributedConfigKeeper::PushConfigToDynamicNode(const TDynamicConfigSubscriber& subscriber,
            TActorId sessionId, bool addCache) {
        auto ev = std::make_unique<TEvNodeWardenDynamicConfigPush>();
        auto& record = ev->Record;
        const auto& config = SelfManagementEnabled ? StorageConfig : BaseConfig;
        if (!PartOfNodeQuorum()) { // this configuration is not reliable, don't push anything
            ev->Record.SetNoQuorum(true);
        } else if (!StorageConfig) {
            // no storage configuration -- no nothing
        } else if (const ui32 required = GetRequiredBridgeFeatureLevel(*config);
                (config->HasClusterState() && !subscriber.BridgeCapable) || subscriber.BridgeFeatureLevel < required) {
            YDB_LOG_WARN("Dynamic node cannot apply the bridged configuration",
                {"marker", "NWDC88"},
                {"nodeId", subscriber.ActorId.NodeId()},
                {"bridgeCapable", subscriber.BridgeCapable},
                {"bridgeFeatureLevel", subscriber.BridgeFeatureLevel},
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
        auto handle = std::make_unique<IEventHandle>(subscriber.ActorId, SelfId(), ev.release());
        handle->Rewrite(TEvInterconnect::EvForward, sessionId);
        TActivationContext::Send(handle.release());
    }

} // NKikimr::NStorage
