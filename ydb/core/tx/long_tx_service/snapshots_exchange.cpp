#include "snapshots_exchange.h"
#include "snapshots_storage.h"

#include <memory>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/services/services.pb.h>
#include <library/cpp/time_provider/time_provider.h>

#define TXLOG_LOG(priority, stream) \
    LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::LONG_TX_SERVICE, LogPrefix << stream)
#define TXLOG_DEBUG(stream) TXLOG_LOG(NActors::NLog::PRI_DEBUG, stream)
#define TXLOG_NOTICE(stream) TXLOG_LOG(NActors::NLog::PRI_NOTICE, stream)
#define TXLOG_ERROR(stream) TXLOG_LOG(NActors::NLog::PRI_ERROR, stream)

namespace NKikimr {
namespace NLongTxService {

namespace {
    class TSubtreeSplitter {
    public:
        TSubtreeSplitter(const ui64 insideDCFanOut, const THashMap<TActorId, TString>& exchangeActorIdToDataCenterId)
            : InsideDCFanOut(insideDCFanOut)
            , ExchangeActorIdToDataCenterId(exchangeActorIdToDataCenterId) {
            AFL_ENSURE(InsideDCFanOut > 0);
        }

        THashMap<TActorId, THashSet<TActorId>> Split(const TVector<TActorId>& children) {
            THashMap<TString, THashSet<TActorId>> dataCenterIdToActorIds;
            TVector<TActorId> unknownDataCenterIdActorIds;
            for (const auto& child : children) {
                if (auto iter = ExchangeActorIdToDataCenterId.find(child); iter != ExchangeActorIdToDataCenterId.end()) {
                    dataCenterIdToActorIds[iter->second].emplace(child);
                } else {
                    unknownDataCenterIdActorIds.push_back(child);
                }
            }

            THashMap<TActorId, THashSet<TActorId>> subtrees;
            if (dataCenterIdToActorIds.size() > 1) {
                // Don't use InsideDCFanOut for cross-dc requests.
                for (const auto& [_, actorIds] : dataCenterIdToActorIds) {
                    const auto& root = actorIds.begin();
                    auto& subtree = subtrees[*root];
                    subtree = actorIds;
                    subtree.erase(*root);
                }

                if (!unknownDataCenterIdActorIds.empty()) {
                    subtrees[unknownDataCenterIdActorIds[0]] = THashSet<TActorId>(std::next(unknownDataCenterIdActorIds.begin()), unknownDataCenterIdActorIds.end());
                }
            } else {
                for (size_t index = 0; index < children.size() && index < InsideDCFanOut; ++index) {
                    AFL_ENSURE(subtrees.emplace(children[index], THashSet<TActorId>{}).second);
                }

                for (size_t index = InsideDCFanOut; index < children.size(); ++index) {
                    subtrees.at(children[index % InsideDCFanOut]).emplace(children[index]);
                }
            }

            return subtrees;
        }

    private:
        ui64 InsideDCFanOut;
        THashMap<TActorId, TString> ExchangeActorIdToDataCenterId;
    };

    template <typename TChildEvent, typename TParentEvent>
    class TTreeNodeActor : public TActorBootstrapped<TTreeNodeActor<TChildEvent, TParentEvent>> {
        using TThis = TTreeNodeActor<TChildEvent, TParentEvent>;
        using TBase = TActorBootstrapped<TThis>;
    public:
        TTreeNodeActor(TActorId parentActorId, TChildEvent* event, const TSubtreeSplitter& subtreeSplitter)
            : ParentActorId(parentActorId)
            , SubtreeSplitter(subtreeSplitter) {
            TVector<TActorId> childrenActorIds;
            childrenActorIds.reserve(event->Record.GetTree().GetChildrenActorIds().size());
            for (const auto& childActorIdProto : event->Record.GetTree().GetChildrenActorIds()) {
                const TActorId childActorId = ActorIdFromProto(childActorIdProto);
                AFL_ENSURE(NodeIdToTreeNodeActorId.emplace(childActorId.NodeId(), childActorId).second);
                childrenActorIds.push_back(childActorId);
            }

            ChildToSubtree = SubtreeSplitter.Split(childrenActorIds);
        }

        void Bootstrap() {
            if (ChildToSubtree.empty()) {
                TXLOG_DEBUG("Leaf node");
                PassAway();
                return;
            }
            for (const auto& [childActorId, subtree] : ChildToSubtree) {
                SendChildEvent(childActorId, subtree);
            }
            TBase::Become(&TThis::StateWork);
        }

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::LONG_TX_SERVICE;
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TParentEvent, Handle);
                hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
            }
        }

        void Handle(TParentEvent::TPtr& ev) {
            AFL_ENSURE(NodeIdToTreeNodeActorId.contains(ev->Sender.NodeId()));
            const auto childActorId = NodeIdToTreeNodeActorId.at(ev->Sender.NodeId());
            TXLOG_DEBUG("Handling TParentEvent from " << ev->Sender
                << " (NodeId: " << ev->Sender.NodeId()
                << ", ChildActorId: " << childActorId << ")");
            if (!ChildToSubtree.contains(childActorId)) {
                return;
            }

            ReceiveFromChild(ev->Get());

            ChildToSubtree.erase(childActorId);
            if (ChildToSubtree.empty()) {
                PassAway();
            }
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
            AFL_ENSURE(NodeIdToTreeNodeActorId.contains(ev->Get()->NodeId));
            const auto failedActorId = NodeIdToTreeNodeActorId.at(ev->Get()->NodeId);
            TXLOG_DEBUG("Handling TEvNodeDisconnected for NodeId: " << ev->Get()->NodeId
                << ". Failed actor ID: " << failedActorId);
            if (!ChildToSubtree.contains(failedActorId)) {
                return;
            }

            if (!RetrySubtree(failedActorId)) {
                if (ChildToSubtree.empty()) {
                    PassAway();
                }
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev) {
            AFL_ENSURE(NodeIdToTreeNodeActorId.contains(ev->Sender.NodeId()));
            const TActorId failedActorId = NodeIdToTreeNodeActorId.at(ev->Sender.NodeId());
            TXLOG_DEBUG("Handling TEvents::TEvUndelivered from " << ev->Sender << ".");
            if (!ChildToSubtree.contains(failedActorId)) {
                return;
            }

            if (!RetrySubtree(failedActorId) && ChildToSubtree.empty()) {
                PassAway();
            }
        }

    private:
        bool RetrySubtree(TActorId childActorId) {
            auto iterChildActorSubtree = ChildToSubtree.find(childActorId);
            AFL_ENSURE(iterChildActorSubtree != ChildToSubtree.end());
            THashSet<TActorId> subtree(std::move(iterChildActorSubtree->second));
            ChildToSubtree.erase(iterChildActorSubtree);
            if (subtree.empty()) {
                return false;
            }
            const TActorId newRoot = *subtree.begin();
            subtree.erase(newRoot);
            AFL_ENSURE(ChildToSubtree.emplace(newRoot, std::move(subtree)).second);

            TXLOG_DEBUG("Retrying subtree for child actor " << childActorId
                << ". New root for subtree: " << newRoot
                << ". Subtree size: " << ChildToSubtree.at(newRoot).size() << ".");
            SendChildEvent(newRoot, ChildToSubtree.at(newRoot));
            return true;
        }

        void SendChildEvent(const TActorId actorId, const THashSet<TActorId>& childActorIds) {
            TXLOG_DEBUG("Sending child event to actor " << actorId << " with " << childActorIds.size() << " children");
            auto event = GetChildEvent();

            for (const auto& childActorId : childActorIds) {
                auto childActorIdProto = event->Record.MutableTree()->AddChildrenActorIds();
                ActorIdToProto(childActorId, childActorIdProto);
            }
        
            TBase::Send(
                actorId,
                event.release(),
                IEventHandle::FlagSubscribeOnSession | IEventHandle::FlagTrackDelivery);
        }

        void PassAway() final {
            TXLOG_DEBUG("Passing away, sending parent event to " << ParentActorId);
            auto event = GetParentEvent();
            TBase::Send(
                ParentActorId,
                event.release());
            TBase::PassAway();
        }

        TActorId ParentActorId;
        TSubtreeSplitter SubtreeSplitter;
        THashMap<TActorId, THashSet<TActorId>> ChildToSubtree;
        THashMap<ui32, TActorId> NodeIdToTreeNodeActorId;
    
    protected:
        TString LogPrefix;

    protected:
        virtual std::unique_ptr<TChildEvent> GetChildEvent() = 0;
        virtual std::unique_ptr<TParentEvent> GetParentEvent() = 0;
        virtual void ReceiveFromChild(TParentEvent*) = 0;
    };

    class TSnapshotCollectorActor : public TTreeNodeActor<TEvLongTxService::TEvCollectSnapshots, TEvLongTxService::TEvCollectSnapshotsResult> {
    public:
        TSnapshotCollectorActor(const TLocalSnapshotsStorage::TView& localSnapshotsView, TActorId parentActorId, TEvLongTxService::TEvCollectSnapshots* event, const TSubtreeSplitter& subtreeSplitter)
            : TTreeNodeActor(parentActorId, event, subtreeSplitter) {
            for (const auto& localSnapshot : localSnapshotsView) {
                TRemoteSnapshotInfo remoteSnapshot(
                    localSnapshot.Snapshot,
                    localSnapshot.SessionActorId,
                    localSnapshot.TableIds);

                AddToCollectedSnapshots(remoteSnapshot);
            }
            LocalCollectionTime = AppData()->TimeProvider->Now();
            TXLOG_DEBUG("Finished creating TSnapshotCollectorActor, local collection time: " << LocalCollectionTime.MilliSeconds());
        }

        std::unique_ptr<TEvLongTxService::TEvCollectSnapshots> GetChildEvent() override {
            return std::make_unique<TEvLongTxService::TEvCollectSnapshots>();
        }

        std::unique_ptr<TEvLongTxService::TEvCollectSnapshotsResult> GetParentEvent() override {
            TXLOG_DEBUG("Creating TEvCollectSnapshotsResult event with "
                << CollectedSnapshots.size() << " collected snapshots and border "
                << SnapshotBorder.Step << ":" << SnapshotBorder.TxId);
            auto event = std::make_unique<TEvLongTxService::TEvCollectSnapshotsResult>();
            event->Record.MutableSnapshots()->SetBorderStep(SnapshotBorder.Step);
            event->Record.MutableSnapshots()->SetBorderTxId(SnapshotBorder.TxId);

            for (const auto& snapshot : CollectedSnapshots) {
                auto* snapshotProto = event->Record.MutableSnapshots()->AddSnapshots();
                snapshotProto->SetSnapshotStep(snapshot.Snapshot.Step);
                snapshotProto->SetSnapshotTxId(snapshot.Snapshot.TxId);
                ActorIdToProto(snapshot.SessionActorId, snapshotProto->MutableSessionActorId());
                for (const auto& tableId : snapshot.TableIds) {
                    auto* tableIdProto = snapshotProto->AddTableIds();
                    tableIdProto->SetOwnerId(tableId.PathId.OwnerId);
                    tableIdProto->SetTableId(tableId.PathId.LocalPathId);
                    tableIdProto->SetSchemaVersion(tableId.SchemaVersion);
                }
            }

            NodesWithCollectionTimes.emplace_back(TNodeWithCollectionTime{
                .NodeId = SelfId().NodeId(),
                .CollectionTime = LocalCollectionTime.Seconds(),
            });

            for (const auto& nodeWithCollectionTime : NodesWithCollectionTimes) {
                auto* nodeInfoProto = event->Record.MutableSnapshots()->AddNodesCollectionInfo();
                nodeInfoProto->SetNodeId(nodeWithCollectionTime.NodeId);
                nodeInfoProto->SetUpdateTime(nodeWithCollectionTime.CollectionTime);
            }

            return std::move(event);
        }

        void ReceiveFromChild(TEvLongTxService::TEvCollectSnapshotsResult* ev) override {
            TRowVersion recvBorder(ev->Record.GetSnapshots().GetBorderStep(), ev->Record.GetSnapshots().GetBorderTxId());
            SnapshotBorder = std::min(SnapshotBorder, recvBorder);
            for (const auto& snapshot : ev->Record.GetSnapshots().GetSnapshots()) {
                NActors::TActorId sessionActorId = ActorIdFromProto(snapshot.GetSessionActorId());
                AFL_ENSURE(sessionActorId);
                TVector<::NKikimr::TTableId> tableIds;
                tableIds.reserve(snapshot.GetTableIds().size());
                for (const auto& tableIdProto : snapshot.GetTableIds()) {
                    tableIds.emplace_back(tableIdProto.GetOwnerId(), tableIdProto.GetTableId(), tableIdProto.GetSchemaVersion());
                }
                
                TRemoteSnapshotInfo remoteSnapshot(
                    TRowVersion(snapshot.GetSnapshotStep(), snapshot.GetSnapshotTxId()),
                    sessionActorId,
                    std::move(tableIds));
                
                AddToCollectedSnapshots(remoteSnapshot);
            }

            for (const auto& nodeCollectionInfo : ev->Record.GetSnapshots().GetNodesCollectionInfo()) {
                NodesWithCollectionTimes.emplace_back(TNodeWithCollectionTime{
                    .NodeId = nodeCollectionInfo.GetNodeId(),
                    .CollectionTime = nodeCollectionInfo.GetUpdateTime(),
                });
            }

            TXLOG_DEBUG("Received TEvCollectSnapshotsResult from child with" << ev->Record.GetSnapshots().GetSnapshots().size()
                << " snapshots. Nodes count: " << ev->Record.GetSnapshots().GetNodesCollectionInfo().size()
                << ". Updated border to " << SnapshotBorder.Step << ":" << SnapshotBorder.TxId);
        }

    private:
        struct TReverseComparatorBySnapshotAndSessionId {
            bool operator()(const TRemoteSnapshotInfo& lhs, const TRemoteSnapshotInfo& rhs) const {
                return std::tie(lhs.Snapshot, lhs.SessionActorId) < std::tie(rhs.Snapshot, rhs.SessionActorId);
            }
        };

        void AddToCollectedSnapshots(const TRemoteSnapshotInfo& snapshot) {
            AFL_ENSURE(CollectedSnapshots.insert(snapshot).second);

            if (CollectedSnapshots.size() >= AppData()->LongTxServiceConfig.GetMaxRemoteSnapshots()) {
                auto lastSnapshotIter = CollectedSnapshots.begin();
                SnapshotBorder = std::min(SnapshotBorder, lastSnapshotIter->Snapshot);
                CollectedSnapshots.erase(lastSnapshotIter);
            }
        }

        struct TNodeWithCollectionTime {
            ui32 NodeId;
            ui64 CollectionTime;
        };
        TVector<TNodeWithCollectionTime> NodesWithCollectionTimes;
        TInstant LocalCollectionTime;
        TSet<TRemoteSnapshotInfo, TReverseComparatorBySnapshotAndSessionId> CollectedSnapshots;
        TRowVersion SnapshotBorder = TRowVersion::Max();
    };

    class TSnapshotPropagatorActor : public TTreeNodeActor<TEvLongTxService::TEvPropagateSnapshots, TEvLongTxService::TEvPropagateSnapshotsResult> {
    public:
        TSnapshotPropagatorActor(TActorId parentActorId, TEvLongTxService::TEvPropagateSnapshots* event, const TSubtreeSplitter& subtreeSplitter)
            : TTreeNodeActor(parentActorId, event, subtreeSplitter) {
            Snapshots_ = event->Record.GetSnapshots();
            TXLOG_DEBUG("Initialized propagator actor with " << Snapshots_.GetSnapshots().size()
                << " snapshots and border " << Snapshots_.GetBorderStep()
                << ":" << Snapshots_.GetBorderTxId());
        }

        std::unique_ptr<TEvLongTxService::TEvPropagateSnapshots> GetChildEvent() override {
            auto event = std::make_unique<TEvLongTxService::TEvPropagateSnapshots>();
            *event->Record.MutableSnapshots() = Snapshots_;
            return std::move(event);
        }

        std::unique_ptr<TEvLongTxService::TEvPropagateSnapshotsResult> GetParentEvent() override {
            return std::make_unique<TEvLongTxService::TEvPropagateSnapshotsResult>();
        }

        void ReceiveFromChild(TEvLongTxService::TEvPropagateSnapshotsResult*) override {
        }

    private:
        NKikimrLongTxService::TRemoteSnapshots Snapshots_;
    };
}

IActor* CreateSnapshotCollectorActor(
        const TLocalSnapshotsStorage::TView& localSnapshotsView,
        TActorId parentActorId,
        TEvLongTxService::TEvCollectSnapshots* event,
        const TSubtreeSplitter& subtreeSplitter) {
    return new TSnapshotCollectorActor(localSnapshotsView, parentActorId, event, subtreeSplitter);
}

IActor* CreateSnapshotPropagatorActor(
        TActorId parentActorId,
        TEvLongTxService::TEvPropagateSnapshots* event,
        const TSubtreeSplitter& subtreeSplitter) {
    return new TSnapshotPropagatorActor(parentActorId, event, subtreeSplitter);
}

class TSnapshotsExchangerActor : public TActorBootstrapped<TSnapshotsExchangerActor> {
    using TThis = TSnapshotsExchangerActor;
    using TBase = TActorBootstrapped<TThis>;

    static TString MakeSnapshotsExchangerBoardPath(TStringBuf database) {
        return TStringBuilder() << "snapshotexch+" << database;
    }

    struct TEvPrivate {
        enum EEv {
            EvRemoteSnapshotsUpdate = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvRemoteSnapshotsUpdate : public TEventLocal<TEvRemoteSnapshotsUpdate, EvRemoteSnapshotsUpdate> {
            TEvRemoteSnapshotsUpdate() = default;
        };
    };

public:
    TSnapshotsExchangerActor(
            TConstLocalSnapshotsStoragePtr localSnapshotsStorage,
            TRemoteSnapshotsStoragePtr remoteSnapshotsStorage,
            TSnapshotExchangeCounters counters)
        : BoardPath(MakeSnapshotsExchangerBoardPath(AppData()->TenantName))
        , LocalSnapshotsStorage(std::move(localSnapshotsStorage))
        , RemoteSnapshotsStorage(std::move(remoteSnapshotsStorage))
        , Counters(std::move(counters)) {
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "TSnapshotsExchangerActor [Node " << SelfId().NodeId() << "] ";
        TXLOG_DEBUG("Creating TSnapshotsExchangerActor with board path: " << BoardPath);
        UpdateBoardRetrySettings();
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));
        TBase::Become(&TThis::StatePrepare);
    }

    void PassAway() {
        TXLOG_DEBUG("Passing away TSnapshotsExchangerActor");
        if (Publisher) {
            Send(Publisher, new TEvents::TEvPoison);
        }
        if (Subscriber) {
            Send(Subscriber, new TEvents::TEvPoison);
        }
        TBase::PassAway();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::LONG_TX_SERVICE;
    }

    STFUNC(StatePrepare) {
        switch (ev->GetTypeRewrite()) {    
            hFunc(TEvInterconnect::TEvNodeInfo, Handle);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {    
            hFunc(TEvLongTxService::TEvCollectSnapshots, Handle);
            hFunc(TEvLongTxService::TEvCollectSnapshotsResult, Handle);
            hFunc(TEvLongTxService::TEvPropagateSnapshots, Handle);
            hFunc(TEvLongTxService::TEvPropagateSnapshotsResult, Handle);

            hFunc(TEvInterconnect::TEvNodeInfo, Handle);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvStateStorage::TEvBoardInfoUpdate, Handle);

            hFunc(TEvPrivate::TEvRemoteSnapshotsUpdate, Handle);
        }
    }

private:
    void Handle(TEvInterconnect::TEvNodeInfo::TPtr& ev) {
        if (const auto& node = ev->Get()->Node) {
            SelfDataCenterId = node->Location.GetDataCenterId();
            TXLOG_DEBUG("Self data center ID: " << SelfDataCenterId);
        } else {
            SelfDataCenterId = TString();
            TXLOG_DEBUG("No node info, setting empty data center ID");
        }

        NKikimrLongTxService::TSnapshotExchangeBoardNodeInfo info;
        info.SetDataCenterId(SelfDataCenterId);
        ActorIdToProto(SelfId(), info.MutableOwner());
        SelfBoardInfo = info.SerializeAsString();

        CreateSubscriber();

        TBase::Become(&TThis::StateWork);
    }

    void UpdateBoardRetrySettings() {
        const auto& longTxConfig = AppData()->LongTxServiceConfig;

        if (longTxConfig.HasPublisherSettings()) {
            const auto& publisherSettings = longTxConfig.GetPublisherSettings();
            if (publisherSettings.HasStartDelayMs()) {
                PublisherSettings.StartDelayMs = TDuration::MilliSeconds(publisherSettings.GetStartDelayMs());
            }
            if (publisherSettings.HasMaxDelayMs()) {
                PublisherSettings.MaxDelayMs = TDuration::MilliSeconds(publisherSettings.GetMaxDelayMs());
            }
        }

        if (longTxConfig.HasSubscriberSettings()) {
            const auto& subscriberSettings = longTxConfig.GetSubscriberSettings();
            if (subscriberSettings.HasStartDelayMs()) {
                SubscriberSettings.StartDelayMs = TDuration::MilliSeconds(subscriberSettings.GetStartDelayMs());
            }
            if (subscriberSettings.HasMaxDelayMs()) {
                SubscriberSettings.MaxDelayMs = TDuration::MilliSeconds(subscriberSettings.GetMaxDelayMs());
            }
        }
    }

    void CreatePublisher() {
        if (Publisher) {
            Send(Publisher, new TEvents::TEvPoison);
            Publisher = TActorId{};
        }

        auto publisher = CreateBoardPublishActor(BoardPath, SelfBoardInfo, SelfId(),
            /* ttlMs */ 0, /* reg */ true, PublisherSettings);
        Publisher = Register(publisher);
    }

    void CreateSubscriber() {
        if (Subscriber) {
            Send(Subscriber, new TEvents::TEvPoison);
            Subscriber = TActorId{};
        }

        auto subscriber = CreateBoardLookupActor(
            BoardPath, SelfId(), EBoardLookupMode::Subscription,
            SubscriberSettings);
        Subscriber = Register(subscriber);
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        if (ev->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable) {
            CreateSubscriber();
            return;
        }

        UpdateNodes(ev->Get()->InfoEntries);
        if (!ExchangeActorsReady) {
            CreatePublisher();
            ExchangeActorsReady = true;
            Send(SelfId(), new TEvPrivate::TEvRemoteSnapshotsUpdate());
        }
    }

    void Handle(TEvStateStorage::TEvBoardInfoUpdate::TPtr& ev) {
        if (ev->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable) {
            CreateSubscriber();
            return;
        }
        UpdateNodes(ev->Get()->Updates);
    }

    void UpdateNodes(const TMap<TActorId, TEvStateStorage::TBoardInfoEntry>& infos) {
        for (const auto& [publisherId, entry] : infos) {
            if (publisherId.NodeId() != SelfId().NodeId()) {
                if (entry.Dropped) {
                    auto iter = PublisherIdToExchangeActorId.find(publisherId);
                    if (iter != PublisherIdToExchangeActorId.end()) {
                        ExchangeActorIdToDataCenterId.erase(iter->second);
                        PublisherIdToExchangeActorId.erase(iter);
                    }
                } else {
                    NKikimrLongTxService::TSnapshotExchangeBoardNodeInfo nodeInfo;
                    Y_PROTOBUF_SUPPRESS_NODISCARD nodeInfo.ParseFromString(entry.Payload);
                    auto owner = ActorIdFromProto(nodeInfo.GetOwner());
                    AFL_ENSURE(owner.NodeId() == publisherId.NodeId());
                    ExchangeActorIdToDataCenterId.emplace(owner, nodeInfo.GetDataCenterId());
                    PublisherIdToExchangeActorId.emplace(publisherId, owner);
                }
            }
        }
        AFL_ENSURE(PublisherIdToExchangeActorId.size() == ExchangeActorIdToDataCenterId.size());
        TXLOG_DEBUG("Finished updating nodes, now know " << ExchangeActorIdToDataCenterId.size() << " other exchange actors");
    }

    void Handle(TEvLongTxService::TEvCollectSnapshots::TPtr& ev) {
        TXLOG_DEBUG("Handling TEvCollectSnapshots event from " << ev->Sender
            << " with " << ev->Get()->Record.GetTree().GetChildrenActorIds().size() << " children");
        auto* collectorActor = CreateSnapshotCollectorActor(
            LocalSnapshotsStorage->View(),
            ev->Sender,
            ev->Get(),
            TSubtreeSplitter(
                AppData()->LongTxServiceConfig.GetInsideDataCenterExchangeFanOut(),
                ExchangeActorIdToDataCenterId));
        RegisterWithSameMailbox(collectorActor);
    }

    void Handle(TEvLongTxService::TEvCollectSnapshotsResult::TPtr& ev) {
        TXLOG_DEBUG("Handling TEvCollectSnapshotsResult event from " << ev->Sender
            << " with " << ev->Get()->Record.GetSnapshots().GetSnapshots().size() << " snapshots"
            << " and border " << ev->Get()->Record.GetSnapshots().GetBorderStep()
            << ":" << ev->Get()->Record.GetSnapshots().GetBorderTxId());
        // Finished collecting snapshots from cluster.
        AFL_ENSURE(UpdateInflight);
        AFL_ENSURE(ev->Sender.NodeId() == SelfId().NodeId());

        if (AppData()->FeatureFlags.GetEnableSnapshotsLocking()) {
            auto propagateEvent = std::make_unique<TEvLongTxService::TEvPropagateSnapshots>();
            FillTreeExchangeActors(propagateEvent->Record.MutableTree());

            *propagateEvent->Record.MutableSnapshots() = std::move(*ev->Get()->Record.MutableSnapshots());

            Send(SelfId(), propagateEvent.release());

            const auto now = AppData()->TimeProvider->Now();
            if (Counters.SnapshotsCollectionTimeMs) {
                Counters.SnapshotsCollectionTimeMs->Set((now - CollectionPropagationStarted).MilliSeconds());
            }
            CollectionPropagationStarted = now;
        } else {
            TXLOG_DEBUG("Snapshots locking is disabled, skipping propagation");
        }
    }

    void Handle(TEvLongTxService::TEvPropagateSnapshots::TPtr& ev) {
        TXLOG_DEBUG("Handling TEvPropagateSnapshots event from " << ev->Sender);
        if (AppData()->FeatureFlags.GetEnableSnapshotsLocking()) {
            // Update remote snapshots storage and continue propagation
            THashMap<ui32, TInstant> nodeIdToCollectionTime;
            TVector<TRemoteSnapshotInfo> remoteSnapshots;
            for (const auto& snapshot : ev->Get()->Record.GetSnapshots().GetSnapshots()) {
                NActors::TActorId sessionActorId = ActorIdFromProto(snapshot.GetSessionActorId());
                AFL_ENSURE(sessionActorId);
                if (sessionActorId.NodeId() != SelfId().NodeId()) {
                    TVector<::NKikimr::TTableId> tableIds;
                    tableIds.reserve(snapshot.GetTableIds().size());
                    for (const auto& tableIdProto : snapshot.GetTableIds()) {
                        tableIds.emplace_back(tableIdProto.GetOwnerId(), tableIdProto.GetTableId(), tableIdProto.GetSchemaVersion());
                    }
                    
                    remoteSnapshots.emplace_back(TRemoteSnapshotInfo{
                        TRowVersion(snapshot.GetSnapshotStep(), snapshot.GetSnapshotTxId()),
                        sessionActorId,
                        std::move(tableIds)});
                }
            }

            for (const auto& nodeCollectionInfo : ev->Get()->Record.GetSnapshots().GetNodesCollectionInfo()) {
                if (nodeCollectionInfo.GetNodeId() != SelfId().NodeId()) {
                    nodeIdToCollectionTime[nodeCollectionInfo.GetNodeId()] = TInstant::Seconds(nodeCollectionInfo.GetUpdateTime());
                }
            }

            TRowVersion border(ev->Get()->Record.GetSnapshots().GetBorderStep(), ev->Get()->Record.GetSnapshots().GetBorderTxId());
            TXLOG_DEBUG("Updating remote snapshots storage with " << remoteSnapshots.size()
                << " snapshots from " << nodeIdToCollectionTime.size() << "nodes."
                << " Update border to " << border.Step << ":" << border.TxId);
            RemoteSnapshotsStorage->UpdateBorder(border);
            RemoteSnapshotsStorage->UpdateAndCleanExpired(remoteSnapshots, nodeIdToCollectionTime);

            LastRemoteSnapshotsUpdate = AppData()->TimeProvider->Now();
            if (Counters.TimeSinceLastRemoteSnapshotsUpdateMs) {
                Counters.TimeSinceLastRemoteSnapshotsUpdateMs->Set(0);
            }
        } else {
            TXLOG_DEBUG("Snapshots locking is disabled, skipping remote snapshots update");
        }

        auto* propagatorActor = CreateSnapshotPropagatorActor(
            ev->Sender,
            ev->Get(),
            TSubtreeSplitter(
                AppData()->LongTxServiceConfig.GetInsideDataCenterExchangeFanOut(),
                ExchangeActorIdToDataCenterId));
        RegisterWithSameMailbox(propagatorActor);
    }

    void Handle(TEvLongTxService::TEvPropagateSnapshotsResult::TPtr&) {
        TXLOG_DEBUG("Handling TEvPropagateSnapshotsResult event");
        // Finished propagating snapshots to cluster.
        AFL_ENSURE(UpdateInflight);
        UpdateInflight = false;

        const auto now = AppData()->TimeProvider->Now();
        if (Counters.SnapshotsPropagationTimeMs) {
            Counters.SnapshotsPropagationTimeMs->Set((now - CollectionPropagationStarted).MilliSeconds());
        }
    }

    void Handle(TEvPrivate::TEvRemoteSnapshotsUpdate::TPtr&) {
        AFL_ENSURE(ExchangeActorsReady);

        if (Counters.TimeSinceLastRemoteSnapshotsUpdateMs) {
            Counters.TimeSinceLastRemoteSnapshotsUpdateMs->Set((AppData()->TimeProvider->Now() - LastRemoteSnapshotsUpdate).MilliSeconds());
        }

        const bool isLeader = IsLeader();
        if (AppData()->FeatureFlags.GetEnableSnapshotsLocking() && isLeader && !UpdateInflight) {
            TXLOG_DEBUG("Starting snapshot collection");
            auto collectSnapshotsEvent = std::make_unique<TEvLongTxService::TEvCollectSnapshots>();
            FillTreeExchangeActors(collectSnapshotsEvent->Record.MutableTree());

            Send(SelfId(), collectSnapshotsEvent.release());
            UpdateInflight = true;

            CollectionPropagationStarted = AppData()->TimeProvider->Now();
        } else if (!AppData()->FeatureFlags.GetEnableSnapshotsLocking()) {
            TXLOG_DEBUG("Snapshots locking is disabled, skipping snapshot collection");
        } else if (!isLeader) {
            TXLOG_DEBUG("Not a leader, skipping snapshot collection");
        } else {
            AFL_ENSURE(UpdateInflight);
            TXLOG_DEBUG("Update already in flight, skipping snapshot collection");
        }
        TXLOG_DEBUG("Scheduling next TEvRemoteSnapshotsUpdate in "
            << AppData()->LongTxServiceConfig.GetSnapshotsExchangeIntervalSeconds() << " seconds");
        Schedule(
            TDuration::Seconds(AppData()->LongTxServiceConfig.GetSnapshotsExchangeIntervalSeconds()),
            new TEvPrivate::TEvRemoteSnapshotsUpdate());
    }

    bool IsLeader() const {        
        return ExchangeActorsReady && std::all_of(ExchangeActorIdToDataCenterId.begin(), ExchangeActorIdToDataCenterId.end(), [this](const std::pair<TActorId, TString>& actorIdAndDataCenterId) {
            return actorIdAndDataCenterId.first < SelfId();
        });
    }

    void FillTreeExchangeActors(NKikimrLongTxService::TPropagationTree* tree) {
        for (const auto& [actorId, _] : ExchangeActorIdToDataCenterId) {
            auto* childActorId = tree->AddChildrenActorIds();
            ActorIdToProto(actorId, childActorId);
        }
    }

private:
    const TString BoardPath;
    TConstLocalSnapshotsStoragePtr LocalSnapshotsStorage;
    TRemoteSnapshotsStoragePtr RemoteSnapshotsStorage;
    TSnapshotExchangeCounters Counters;
    TString LogPrefix;

    TActorId Publisher;
    TActorId Subscriber;
    THashMap<TActorId, TString> ExchangeActorIdToDataCenterId;
    THashMap<TActorId, TActorId> PublisherIdToExchangeActorId;
    bool ExchangeActorsReady = false;
    TString SelfDataCenterId;
    TString SelfBoardInfo;

    bool UpdateInflight = false;
    TInstant CollectionPropagationStarted;
    TInstant LastRemoteSnapshotsUpdate;

    TBoardRetrySettings PublisherSettings;
    TBoardRetrySettings SubscriberSettings;
};

IActor* CreateSnapshotExchangeActor(
        TConstLocalSnapshotsStoragePtr localSnapshotsStorage,
        TRemoteSnapshotsStoragePtr remoteSnapshotsStorage,
        TSnapshotExchangeCounters counters) {
    return new TSnapshotsExchangerActor(
        std::move(localSnapshotsStorage),
        std::move(remoteSnapshotsStorage),
        std::move(counters));
}

}
}
