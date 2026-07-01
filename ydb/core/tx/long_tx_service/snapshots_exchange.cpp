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

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::LONG_TX_SERVICE

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::LONG_TX_SERVICE

namespace NKikimr {
namespace NLongTxService {

namespace {
    struct TReverseComparatorBySnapshotAndSessionId {
        bool operator()(const TRemoteSnapshotInfo& lhs, const TRemoteSnapshotInfo& rhs) const {
            return std::tie(lhs.Snapshot, lhs.SessionActorId) > std::tie(rhs.Snapshot, rhs.SessionActorId);
        }
    };

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
            LogPrefix = TStringBuilder() << "TTreeNodeActor [Node " << TBase::SelfId().NodeId() << "] ";
            if (ChildToSubtree.empty()) {
                YDB_LOG_DEBUG("Leaf node",
                    {"logPrefix", LogPrefix});
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
            YDB_LOG_DEBUG("Handling TParentEvent",
                {"logPrefix", LogPrefix},
                {"sender", ev->Sender},
                {"nodeId", ev->Sender.NodeId()},
                {"childActorId", childActorId});
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
            YDB_LOG_DEBUG("Handling TEvNodeDisconnected for Failed actor",
                {"logPrefix", LogPrefix},
                {"nodeId", ev->Get()->NodeId},
                {"ID", failedActorId});
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
            YDB_LOG_DEBUG("Handling TEvents::TEvUndelivered",
                {"logPrefix", LogPrefix},
                {"sender", ev->Sender});
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

            YDB_LOG_DEBUG("Retrying subtree for child actor New root for Subtree",
                {"logPrefix", LogPrefix},
                {"childActorId", childActorId},
                {"subtree", newRoot},
                {"size", ChildToSubtree.at(newRoot).size()});
            SendChildEvent(newRoot, ChildToSubtree.at(newRoot));
            return true;
        }

        void SendChildEvent(const TActorId actorId, const THashSet<TActorId>& childActorIds) {
            YDB_LOG_DEBUG("Sending child event to actor with children",
                {"logPrefix", LogPrefix},
                {"actorId", actorId},
                {"childActorIdsCount", childActorIds.size()});
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
            YDB_LOG_DEBUG("Passing away, sending parent event",
                {"logPrefix", LogPrefix},
                {"parentActorId", ParentActorId});
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
        TSnapshotCollectorActor(const TLocalSnapshotsStorage::TView& localSnapshotsView, TInstant now, TActorId parentActorId, TEvLongTxService::TEvCollectSnapshots* event, const TSubtreeSplitter& subtreeSplitter)
            : TTreeNodeActor(parentActorId, event, subtreeSplitter)
            , LocalCollectionTime(now) {
            for (const auto& localSnapshot : localSnapshotsView) {
                TRemoteSnapshotInfo remoteSnapshot(
                    localSnapshot.Snapshot,
                    localSnapshot.SessionActorId,
                    localSnapshot.TableIds);

                AddToCollectedSnapshots(remoteSnapshot);
            }

            YDB_LOG_DEBUG("Finished creating TSnapshotCollectorActor",
                {"logPrefix", LogPrefix},
                {"localCollectionTime", LocalCollectionTime.MilliSeconds()});
        }

        std::unique_ptr<TEvLongTxService::TEvCollectSnapshots> GetChildEvent() override {
            return std::make_unique<TEvLongTxService::TEvCollectSnapshots>();
        }

        std::unique_ptr<TEvLongTxService::TEvCollectSnapshotsResult> GetParentEvent() override {
            YDB_LOG_DEBUG("Creating TEvCollectSnapshotsResult event with collected snapshots and border",
                {"logPrefix", LogPrefix},
                {"collectedSnapshotsSize", CollectedSnapshots.size()},
                {"snapshotBorderStep", SnapshotBorder.Step},
                {"snapshotBorderTxId", SnapshotBorder.TxId});
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

            YDB_LOG_DEBUG("Received TEvCollectSnapshotsResult from child with snapshots. Nodes Updated border",
                {"logPrefix", LogPrefix},
                {"snapshotsCount", ev->Record.GetSnapshots().GetSnapshots().size()},
                {"nodesCount", ev->Record.GetSnapshots().GetNodesCollectionInfo().size()},
                {"snapshotBorderStep", SnapshotBorder.Step},
                {"snapshotBorderTxId", SnapshotBorder.TxId});
        }

    private:
        void AddToCollectedSnapshots(const TRemoteSnapshotInfo& snapshot) {
            AFL_ENSURE(CollectedSnapshots.insert(snapshot).second);

            if (CollectedSnapshots.size() > AppData()->LongTxServiceConfig.GetMaxRemoteSnapshots()) {
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
            YDB_LOG_DEBUG("Initialized propagator actor with snapshots and border",
                {"logPrefix", LogPrefix},
                {"snapshotsSize", Snapshots_.GetSnapshots().size()},
                {"borderStep", Snapshots_.GetBorderStep()},
                {"borderTxId", Snapshots_.GetBorderTxId()});
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
        TInstant now,
        TActorId parentActorId,
        TEvLongTxService::TEvCollectSnapshots* event,
        const TSubtreeSplitter& subtreeSplitter) {
    return new TSnapshotCollectorActor(localSnapshotsView, now, parentActorId, event, subtreeSplitter);
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
            EvPrefillTimeout,
        };

        struct TEvRemoteSnapshotsUpdate : public TEventLocal<TEvRemoteSnapshotsUpdate, EvRemoteSnapshotsUpdate> {
            TEvRemoteSnapshotsUpdate() = default;
        };

        struct TEvPrefillTimeout : public TEventLocal<TEvPrefillTimeout, EvPrefillTimeout> {
            TEvPrefillTimeout() = default;
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
        YDB_LOG_DEBUG("Creating TSnapshotsExchangerActor with board",
            {"logPrefix", LogPrefix},
            {"path", BoardPath});
        UpdateBoardRetrySettings();
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));
        TBase::Become(&TThis::StatePrepare);
    }

    void PassAway() {
        YDB_LOG_DEBUG("Passing away TSnapshotsExchangerActor",
            {"logPrefix", LogPrefix});
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

            sFunc(TEvents::TEvPoison, HandlePoison);
        }
    }

    STFUNC(StatePrefill) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvStateStorage::TEvBoardInfoUpdate, Handle);

            hFunc(TEvLongTxService::TEvRemoteSnapshotsPrefillResult, Handle);

            // Failed to prefill
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandlePrefill);
            hFunc(TEvents::TEvUndelivered, HandlePrefill);
            hFunc(TEvPrivate::TEvPrefillTimeout, HandlePrefill);

            sFunc(TEvents::TEvPoison, HandlePoison);
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvLongTxService::TEvCollectSnapshots, Handle);
            hFunc(TEvLongTxService::TEvCollectSnapshotsResult, Handle);
            hFunc(TEvLongTxService::TEvPropagateSnapshots, Handle);
            hFunc(TEvLongTxService::TEvPropagateSnapshotsResult, Handle);

            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvStateStorage::TEvBoardInfoUpdate, Handle);

            hFunc(TEvPrivate::TEvRemoteSnapshotsUpdate, Handle);

            hFunc(TEvLongTxService::TEvRemoteSnapshotsPrefill, Handle);

            // Events in case of failed prefill
            IgnoreFunc(TEvLongTxService::TEvRemoteSnapshotsPrefillResult);
            IgnoreFunc(TEvInterconnect::TEvNodeDisconnected);
            IgnoreFunc(TEvents::TEvUndelivered);
            IgnoreFunc(TEvPrivate::TEvPrefillTimeout);

            sFunc(TEvents::TEvPoison, HandlePoison);
        }
    }

private:
    void Handle(TEvInterconnect::TEvNodeInfo::TPtr& ev) {
        if (const auto& node = ev->Get()->Node) {
            SelfDataCenterId = node->Location.GetDataCenterId();
            YDB_LOG_DEBUG("Self data center",
                {"logPrefix", LogPrefix},
                {"ID", SelfDataCenterId});
        } else {
            SelfDataCenterId = TString();
            YDB_LOG_DEBUG("No node info, setting empty data center ID",
                {"logPrefix", LogPrefix});
        }

        NKikimrLongTxService::TSnapshotExchangeBoardNodeInfo info;
        info.SetDataCenterId(SelfDataCenterId);
        ActorIdToProto(SelfId(), info.MutableOwner());
        SelfBoardInfo = info.SerializeAsString();

        CreateSubscriber();
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

        if (CurrentStateFunc() == &TThis::StatePrepare) {
            if (!PublisherIdToExchangeActorId.empty()) {
                StartPrefill();
            } else {
                ProceedToPublish();
            }
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
        YDB_LOG_DEBUG("Finished updating nodes, now know other exchange actors",
            {"logPrefix", LogPrefix},
            {"exchangeActorIdToDataCenterIdSize", ExchangeActorIdToDataCenterId.size()});
    }

    void Handle(TEvLongTxService::TEvCollectSnapshots::TPtr& ev) {
        YDB_LOG_DEBUG("Handling TEvCollectSnapshots event with children",
            {"logPrefix", LogPrefix},
            {"sender", ev->Sender},
            {"childrenActorIdsSize", ev->Get()->Record.GetTree().GetChildrenActorIds().size()});

        const auto now = AppData()->TimeProvider->Now();
        auto* collectorActor = CreateSnapshotCollectorActor(
            LocalSnapshotsStorage->View(now),
            now,
            ev->Sender,
            ev->Get(),
            TSubtreeSplitter(
                AppData()->LongTxServiceConfig.GetInsideDataCenterExchangeFanOut(),
                ExchangeActorIdToDataCenterId));
        RegisterWithSameMailbox(collectorActor);
    }

    void Handle(TEvLongTxService::TEvCollectSnapshotsResult::TPtr& ev) {
        YDB_LOG_DEBUG("Handling TEvCollectSnapshotsResult event from with snapshots and border",
            {"logPrefix", LogPrefix},
            {"sender", ev->Sender},
            {"snapshotsSize", ev->Get()->Record.GetSnapshots().GetSnapshots().size()},
            {"borderStep", ev->Get()->Record.GetSnapshots().GetBorderStep()},
            {"borderTxId", ev->Get()->Record.GetSnapshots().GetBorderTxId()});
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
            YDB_LOG_DEBUG("Snapshots locking is disabled, skipping propagation",
                {"logPrefix", LogPrefix});
        }
    }

    void Handle(TEvLongTxService::TEvPropagateSnapshots::TPtr& ev) {
        YDB_LOG_DEBUG("Handling TEvPropagateSnapshots event",
            {"logPrefix", LogPrefix},
            {"sender", ev->Sender});
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
            YDB_LOG_DEBUG("Updating remote snapshots storage with snapshots from nodes. Update border",
                {"logPrefix", LogPrefix},
                {"remoteSnapshotsSize", remoteSnapshots.size()},
                {"nodeIdToCollectionTimeSize", nodeIdToCollectionTime.size()},
                {"borderStep", border.Step},
                {"borderTxId", border.TxId});
            RemoteSnapshotsStorage->UpdateBorder(border);
            std::sort(std::begin(remoteSnapshots), std::end(remoteSnapshots),
                TRemoteSnapshotInfo::TComparatorBySnapshotAndSessionId{});
            RemoteSnapshotsStorage->UpdateAndCleanExpired(remoteSnapshots, nodeIdToCollectionTime);

            LastRemoteSnapshotsUpdate = AppData()->TimeProvider->Now();
            if (Counters.TimeSinceLastRemoteSnapshotsUpdateMs) {
                Counters.TimeSinceLastRemoteSnapshotsUpdateMs->Set(0);
            }
        } else {
            YDB_LOG_DEBUG("Snapshots locking is disabled, skipping remote snapshots update",
                {"logPrefix", LogPrefix});
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
        YDB_LOG_DEBUG("Handling TEvPropagateSnapshotsResult event",
            {"logPrefix", LogPrefix});
        // Finished propagating snapshots to cluster.
        AFL_ENSURE(UpdateInflight);
        UpdateInflight = false;

        const auto now = AppData()->TimeProvider->Now();
        if (Counters.SnapshotsPropagationTimeMs) {
            Counters.SnapshotsPropagationTimeMs->Set((now - CollectionPropagationStarted).MilliSeconds());
        }
    }

    void Handle(TEvLongTxService::TEvRemoteSnapshotsPrefill::TPtr& ev) {
        YDB_LOG_DEBUG("Handling TEvRemoteSnapshotsPrefill",
            {"logPrefix", LogPrefix},
            {"sender", ev->Sender});

        auto resultEvent = std::make_unique<TEvLongTxService::TEvRemoteSnapshotsPrefillResult>();

        TSet<TRemoteSnapshotInfo, TReverseComparatorBySnapshotAndSessionId> collectedSnapshots;
        TRowVersion snapshotBorder = TRowVersion::Max();

        auto addToCollectedSnapshots = [&collectedSnapshots, &snapshotBorder](const TRemoteSnapshotInfo& snapshot) {
            AFL_ENSURE(collectedSnapshots.insert(snapshot).second);
            if (collectedSnapshots.size() > AppData()->LongTxServiceConfig.GetMaxRemoteSnapshots()) {
                auto lastSnapshotIter = collectedSnapshots.begin();
                snapshotBorder = std::min(snapshotBorder, lastSnapshotIter->Snapshot);
                collectedSnapshots.erase(lastSnapshotIter);
            }
        };

        for (const auto& remoteSnapshot : RemoteSnapshotsStorage->View()) {
            addToCollectedSnapshots(remoteSnapshot);
        }

        const auto now = AppData()->TimeProvider->Now();
        for (const auto& localSnapshot : LocalSnapshotsStorage->View(now)) {
            TRemoteSnapshotInfo remoteSnapshot(
                localSnapshot.Snapshot,
                localSnapshot.SessionActorId,
                localSnapshot.TableIds);

            addToCollectedSnapshots(remoteSnapshot);
        }

        for (const auto& remoteSnapshot : collectedSnapshots) {
            auto* snapshotProto = resultEvent->Record.MutableSnapshots()->AddSnapshots();
            snapshotProto->SetSnapshotStep(remoteSnapshot.Snapshot.Step);
            snapshotProto->SetSnapshotTxId(remoteSnapshot.Snapshot.TxId);
            ActorIdToProto(remoteSnapshot.SessionActorId, snapshotProto->MutableSessionActorId());
            for (const auto& tableId : remoteSnapshot.TableIds) {
                auto* tableIdProto = snapshotProto->AddTableIds();
                tableIdProto->SetOwnerId(tableId.PathId.OwnerId);
                tableIdProto->SetTableId(tableId.PathId.LocalPathId);
                tableIdProto->SetSchemaVersion(tableId.SchemaVersion);
            }
        }

        resultEvent->Record.MutableSnapshots()->SetBorderStep(snapshotBorder.Step);
        resultEvent->Record.MutableSnapshots()->SetBorderTxId(snapshotBorder.TxId);

        // Convey per-node collection times so the receiver's registry freshness (OldestCollectionTime)
        // is meaningful right after prefill, instead of treating all prefilled nodes as unknown (Zero).
        for (const auto& [nodeId, collectionTime] : RemoteSnapshotsStorage->GetNodeIdToCollectionTime()) {
            auto* nodeInfoProto = resultEvent->Record.MutableSnapshots()->AddNodesCollectionInfo();
            nodeInfoProto->SetNodeId(nodeId);
            nodeInfoProto->SetUpdateTime(collectionTime.Seconds());
        }
        // This node's own snapshots are known as of now.
        auto* localNodeInfoProto = resultEvent->Record.MutableSnapshots()->AddNodesCollectionInfo();
        localNodeInfoProto->SetNodeId(SelfId().NodeId());
        localNodeInfoProto->SetUpdateTime(now.Seconds());

        YDB_LOG_DEBUG("Sending TEvRemoteSnapshotsPrefillResult to with snapshots",
            {"logPrefix", LogPrefix},
            {"sender", ev->Sender},
            {"snapshotsSize", resultEvent->Record.GetSnapshots().GetSnapshots().size()});
        Send(ev->Sender, resultEvent.release());
    }

    void Handle(TEvPrivate::TEvRemoteSnapshotsUpdate::TPtr&) {
        if (Counters.TimeSinceLastRemoteSnapshotsUpdateMs) {
            Counters.TimeSinceLastRemoteSnapshotsUpdateMs->Set((AppData()->TimeProvider->Now() - LastRemoteSnapshotsUpdate).MilliSeconds());
        }

        const bool isLeader = IsLeader();
        if (AppData()->FeatureFlags.GetEnableSnapshotsLocking() && isLeader && !UpdateInflight) {
            YDB_LOG_DEBUG("Starting snapshot collection",
                {"logPrefix", LogPrefix});
            auto collectSnapshotsEvent = std::make_unique<TEvLongTxService::TEvCollectSnapshots>();
            FillTreeExchangeActors(collectSnapshotsEvent->Record.MutableTree());

            Send(SelfId(), collectSnapshotsEvent.release());
            UpdateInflight = true;

            CollectionPropagationStarted = AppData()->TimeProvider->Now();
        } else if (!AppData()->FeatureFlags.GetEnableSnapshotsLocking()) {
            YDB_LOG_DEBUG("Snapshots locking is disabled, skipping snapshot collection",
                {"logPrefix", LogPrefix});
        } else if (!isLeader) {
            YDB_LOG_DEBUG("Not a leader, skipping snapshot collection",
                {"logPrefix", LogPrefix});
        } else {
            AFL_ENSURE(UpdateInflight);
            YDB_LOG_DEBUG("Update already in flight, skipping snapshot collection",
                {"logPrefix", LogPrefix});
        }
        YDB_LOG_DEBUG("Scheduling next TEvRemoteSnapshotsUpdate in seconds",
            {"logPrefix", LogPrefix},
            {"interval", AppData()->LongTxServiceConfig.GetSnapshotsExchangeIntervalSeconds()});
        Schedule(
            TDuration::Seconds(AppData()->LongTxServiceConfig.GetSnapshotsExchangeIntervalSeconds()),
            new TEvPrivate::TEvRemoteSnapshotsUpdate());
    }

    bool IsLeader() const {
        return std::all_of(ExchangeActorIdToDataCenterId.begin(), ExchangeActorIdToDataCenterId.end(), [this](const std::pair<TActorId, TString>& actorIdAndDataCenterId) {
            return actorIdAndDataCenterId.first < SelfId();
        });
    }

    void StartPrefill() {
        AFL_ENSURE(!PublisherIdToExchangeActorId.empty());
        auto actorIt = PublisherIdToExchangeActorId.begin();
        std::advance(actorIt, RandomNumber<ui64>(PublisherIdToExchangeActorId.size()));
        PrefillTargetActor = actorIt->second;
        YDB_LOG_DEBUG("Requesting prefill from random",
            {"logPrefix", LogPrefix},
            {"peer", PrefillTargetActor});

        auto event = std::make_unique<TEvLongTxService::TEvRemoteSnapshotsPrefill>();
        Send(PrefillTargetActor, event.release(), IEventHandle::FlagTrackDelivery);

        Schedule(
            TDuration::Seconds(AppData()->LongTxServiceConfig.GetPrefillTimeoutSeconds()),
            new TEvPrivate::TEvPrefillTimeout());
        TBase::Become(&TThis::StatePrefill);
    }

    void Handle(TEvLongTxService::TEvRemoteSnapshotsPrefillResult::TPtr& ev) {
        YDB_LOG_DEBUG("Handling TEvRemoteSnapshotsPrefillResult",
            {"logPrefix", LogPrefix},
            {"sender", ev->Sender});
        AFL_ENSURE(ev->Sender == PrefillTargetActor);

        const auto& snapshots = ev->Get()->Record.GetSnapshots();

        TVector<TRemoteSnapshotInfo> remoteSnapshots;
        for (const auto& snapshot : snapshots.GetSnapshots()) {
            NActors::TActorId sessionActorId = ActorIdFromProto(snapshot.GetSessionActorId());
            AFL_ENSURE(sessionActorId);
            // RemoteSnapshotsStorage tracks other nodes only (own snapshots live in LocalSnapshotsStorage),
            // so skip our own node here, consistently with collection times below and the propagation path.
            if (sessionActorId.NodeId() == SelfId().NodeId()) {
                continue;
            }
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

        THashMap<ui32, TInstant> nodeIdToCollectionTime;
        for (const auto& nodeCollectionInfo : snapshots.GetNodesCollectionInfo()) {
            if (nodeCollectionInfo.GetNodeId() != SelfId().NodeId()) {
                nodeIdToCollectionTime[nodeCollectionInfo.GetNodeId()] = TInstant::Seconds(nodeCollectionInfo.GetUpdateTime());
            }
        }

        TRowVersion border(snapshots.GetBorderStep(), snapshots.GetBorderTxId());
        YDB_LOG_DEBUG("Applying prefill remote border",
            {"logPrefix", LogPrefix},
            {"snapshots", remoteSnapshots.size()},
            {"borderStep", border.Step},
            {"borderTxId", border.TxId});

        RemoteSnapshotsStorage->UpdateBorder(border);
        std::sort(std::begin(remoteSnapshots), std::end(remoteSnapshots),
            TRemoteSnapshotInfo::TComparatorBySnapshotAndSessionId{});
        RemoteSnapshotsStorage->Init(remoteSnapshots, nodeIdToCollectionTime);
        LastRemoteSnapshotsUpdate = AppData()->TimeProvider->Now();

        ProceedToPublish();
    }

    void HandlePrefill(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        YDB_LOG_DEBUG("Handling TEvNodeDisconnected",
            {"logPrefix", LogPrefix},
            {"nodeId", ev->Get()->NodeId});
        YDB_LOG_DEBUG("Prefill target disconnected, proceeding without prefill",
            {"logPrefix", LogPrefix});
        ProceedToPublish();
    }

    void HandlePrefill(TEvents::TEvUndelivered::TPtr& ev) {
        YDB_LOG_DEBUG("Handling TEvents::TEvUndelivered",
            {"logPrefix", LogPrefix},
            {"sender", ev->Sender});
        YDB_LOG_DEBUG("Prefill request undelivered, proceeding without prefill",
            {"logPrefix", LogPrefix});
        ProceedToPublish();
    }

    void HandlePrefill(TEvPrivate::TEvPrefillTimeout::TPtr&) {
        YDB_LOG_DEBUG("Prefill timed out, proceeding without prefill",
            {"logPrefix", LogPrefix});
        ProceedToPublish();
    }

    void ProceedToPublish() {
        CreatePublisher();
        Send(SelfId(), new TEvPrivate::TEvRemoteSnapshotsUpdate());
        Become(&TThis::StateWork);
    }

    void FillTreeExchangeActors(NKikimrLongTxService::TPropagationTree* tree) {
        for (const auto& [actorId, _] : ExchangeActorIdToDataCenterId) {
            auto* childActorId = tree->AddChildrenActorIds();
            ActorIdToProto(actorId, childActorId);
        }
    }

    void HandlePoison() {
        PassAway();
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
    TString SelfDataCenterId;
    TString SelfBoardInfo;

    TActorId PrefillTargetActor;

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
