#include <ydb/core/tx/long_tx_service/snapshots_exchange.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NLongTxService {
namespace {

using EBoardStatus = TEvStateStorage::TEvBoardInfo::EStatus;

class TSnapshotSubscriptionTest {
public:
    explicit TSnapshotSubscriptionTest(size_t groups = 1, ui32 startDelayMs = 100, ui32 maxDelayMs = 100,
        ui32 nodeCount = 2)
        : Runtime(nodeCount)
        , LocalStorage(MakeIntrusive<TLocalSnapshotsStorage>())
        , RemoteStorage(MakeIntrusive<TRemoteSnapshotsStorage>())
        , BoardConfig(MakeIntrusive<TStateStorageInfo>())
    {
        Runtime.SetDispatchedEventsLimit(20000);
        Runtime.SetFastSimulatedTime(true);
        BoardConfig->RingGroups.resize(groups);
        for (size_t groupIndex = 0; groupIndex < groups; ++groupIndex) {
            auto& group = BoardConfig->RingGroups[groupIndex];
            group.State = groupIndex == 0 ? PRIMARY : SYNCHRONIZED;
            group.NToSelect = 3;
            group.Rings.resize(3);
            for (size_t index = 0; index < 3; ++index) {
                const ui32 replicaIndex = groupIndex * 3 + index;
                const auto replica = MakeBoardReplicaID(Runtime.GetNodeId(), replicaIndex);
                Replicas.push_back(replica);
                group.Rings[index].Replicas.push_back(replica);
                Runtime.AddLocalService(replica,
                                        TActorSetupCmd(CreateStateStorageBoardReplica(BoardConfig, replicaIndex), TMailboxType::Simple, 0));
            }
        }
        for (ui32 nodeIndex = 0; nodeIndex < Runtime.GetNodeCount(); ++nodeIndex) {
            Runtime.AddLocalService(MakeStateStorageProxyID(),
                                    TActorSetupCmd(CreateStateStorageProxy(BoardConfig, BoardConfig, BoardConfig), TMailboxType::Simple, 0),
                                    nodeIndex);
        }
        Runtime.Initialize(TAppPrepare().Unwrap());
        for (ui32 nodeIndex = 0; nodeIndex < Runtime.GetNodeCount(); ++nodeIndex) {
            auto& appData = Runtime.GetAppData(nodeIndex);
            appData.TenantName = "/snapshot-subscription-test";
            appData.FeatureFlags.SetEnableSnapshotsLocking(false);
            auto& config = appData.LongTxServiceConfig;
            config.SetSnapshotsExchangeIntervalSeconds(3600);
            config.SetPrefillTimeoutSeconds(30);
            config.MutableSubscriberSettings()->SetStartDelayMs(startDelayMs);
            config.MutableSubscriberSettings()->SetMaxDelayMs(maxDelayMs);
        }
        Peer = Runtime.AllocateEdgeActor(1);

        Runtime.SetEventFilter([this](auto&, auto& ev) {
            if (ev->GetTypeRewrite() == TEvStateStorage::TEvReplicaBoardLookup::EventType) {
                Subscribers.insert(ev->Sender);
            } else if (ev->Recipient == Exchanger && ev->Cookie != InjectedCookie) {
                if (ev->GetTypeRewrite() == TEvStateStorage::TEvBoardInfo::EventType) {
                    ObserveResult(ev->Sender, ev->template Get<TEvStateStorage::TEvBoardInfo>()->Status);
                } else if (ev->GetTypeRewrite() == TEvStateStorage::TEvBoardInfoUpdate::EventType &&
                           ev->template Get<TEvStateStorage::TEvBoardInfoUpdate>()->Status == EBoardStatus::NotAvailable) {
                    ObserveResult(ev->Sender, EBoardStatus::NotAvailable);
                }
            } else if (ev->GetTypeRewrite() == TEvStateStorage::TEvReplicaBoardPublish::EventType &&
                       ev->Sender.NodeId() == Runtime.GetNodeId()) {
                ++LocalPublishRequests;
            }
            if (ev->Recipient == Peer && ev->GetTypeRewrite() == TEvLongTxService::TEvRemoteSnapshotsPrefill::EventType) {
                ++PrefillRequests;
                return true;
            }
            if (ev->Recipient == Peer && ev->GetTypeRewrite() == TEvLongTxService::TEvCollectSnapshots::EventType) {
                ++CollectRequests;
            }
            return false;
        });
    }

    void Start() {
        Runtime.RunCall([this] {
            Exchanger = Runtime.Register(CreateSnapshotExchangeActor(LocalStorage, RemoteStorage, {}));
            Runtime.EnableScheduleForActor(Exchanger);
            return true;
        });
    }

    void StartHealthy() {
        Start();
        WaitForSuccess(1);
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    }

    void StopReplicas(std::initializer_list<size_t> indices) {
        for (const auto index : indices) {
            Runtime.Send(new IEventHandle(Replicas.at(index), {}, new TEvents::TEvPoisonPill()), 0, true);
        }
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    }

    void RestoreReplicas(std::initializer_list<size_t> indices) {
        for (const auto index : indices) {
            const auto replica = Runtime.Register(CreateStateStorageBoardReplica(BoardConfig, index));
            Runtime.RegisterService(Replicas.at(index), replica);
        }
    }

    void DisconnectSecondGroup() {
        auto config = MakeIntrusive<TStateStorageInfo>();
        config->RingGroups = BoardConfig->RingGroups;
        config->RingGroups.at(1).State = DISCONNECTED;
        BoardConfig = config;
        for (ui32 nodeIndex = 0; nodeIndex < Runtime.GetNodeCount(); ++nodeIndex) {
            Runtime.Send(new IEventHandle(MakeStateStorageProxyID(), {}, new TEvStateStorage::TEvUpdateGroupConfig(config, config, config)),
                         nodeIndex,
                         true);
        }
    }

    void WaitForFailure(size_t count) {
        Runtime.WaitFor("board subscription failure", [&] { return FailureTimes.size() >= count; }, TDuration::Seconds(3));
    }

    void WaitForSuccess(size_t count) {
        Runtime.WaitFor("board subscription recovery", [&] { return Successes >= count; }, TDuration::Seconds(3));
    }

    void CheckCooldown() {
        const auto attempts = Subscribers.size();
        Runtime.SimulateSleep(TDuration::MilliSeconds(40));
        UNIT_ASSERT_VALUES_EQUAL(Subscribers.size(), attempts);
    }

    TActorId PublishPeer() {
        NKikimrLongTxService::TSnapshotExchangeBoardNodeInfo info;
        info.SetDataCenterId("peer-dc");
        ActorIdToProto(Peer, info.MutableOwner());
        const auto publisher = Runtime.Register(CreateBoardPublishActor(BoardPath, info.SerializeAsString(), Peer, 0, true), 1);
        Runtime.EnableScheduleForActor(publisher);
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        return publisher;
    }

    void FinishPrefill() {
        auto* reply = new TEvLongTxService::TEvRemoteSnapshotsPrefillResult();
        auto* snapshots = reply->Record.MutableSnapshots();
        snapshots->SetBorderStep(1000);
        snapshots->SetBorderTxId(1);
        auto* snapshot = snapshots->AddSnapshots();
        snapshot->SetSnapshotStep(100);
        snapshot->SetSnapshotTxId(1);
        ActorIdToProto(Peer, snapshot->MutableSessionActorId());
        auto* node = snapshots->AddNodesCollectionInfo();
        node->SetNodeId(Peer.NodeId());
        node->SetUpdateTime(Runtime.GetCurrentTime().Seconds());
        Runtime.Send(new IEventHandle(Exchanger, Peer, reply), 1, true);
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    }

    void CheckPrefilledSnapshots() {
        UNIT_ASSERT(RemoteStorage->IsReady());
        UNIT_ASSERT_VALUES_EQUAL(RemoteStorage->GetBorder(), TRowVersion(1000, 1));
        size_t count = 0;
        for (const auto& snapshot : RemoteStorage->View()) {
            UNIT_ASSERT_VALUES_EQUAL(snapshot.SessionActorId, Peer);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.Snapshot, TRowVersion(100, 1));
            ++count;
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 1);
    }

    void SendBoardInfo(const TActorId& sender, EBoardStatus status) {
        Runtime.Send(new IEventHandle(Exchanger, sender, new TEvStateStorage::TEvBoardInfo(status, BoardPath), 0, InjectedCookie), 0, true);
    }

    void SendBoardUpdate(const TActorId& sender, EBoardStatus status) {
        auto* update = new TEvStateStorage::TEvBoardInfoUpdate(status, BoardPath);
        if (status == EBoardStatus::Ok) {
            NKikimrLongTxService::TSnapshotExchangeBoardNodeInfo info;
            ActorIdToProto(Peer, info.MutableOwner());
            update->Updates.emplace(Peer, TEvStateStorage::TBoardInfoEntry{info.SerializeAsString(), false});
        }
        Runtime.Send(new IEventHandle(Exchanger, sender, update, 0, InjectedCookie), 0, true);
    }

    TTestBasicRuntime Runtime;
    TActorId Exchanger;
    TActorId Peer;
    TActorId LastFailedSubscriber;
    THashSet<TActorId> Subscribers;
    TVector<TInstant> FailureTimes;
    size_t Successes = 0;
    size_t LocalPublishRequests = 0;
    size_t PrefillRequests = 0;
    size_t CollectRequests = 0;
    TLocalSnapshotsStoragePtr LocalStorage;
    TRemoteSnapshotsStoragePtr RemoteStorage;
    static inline const TString BoardPath = "snapshotexch+/snapshot-subscription-test";

private:
    void ObserveResult(const TActorId& sender, EBoardStatus status) {
        if (status == EBoardStatus::NotAvailable) {
            LastFailedSubscriber = sender;
            FailureTimes.push_back(Runtime.GetCurrentTime());
        } else {
            ++Successes;
        }
    }

    static constexpr ui64 InjectedCookie = 1;
    TIntrusivePtr<TStateStorageInfo> BoardConfig;
    TVector<TActorId> Replicas;
};

class TSnapshotOverflowTest : public TSnapshotSubscriptionTest {
public:
    using TWireSnapshots = NKikimrLongTxService::TRemoteSnapshots;

    TSnapshotOverflowTest()
        : TSnapshotSubscriptionTest(1, 100, 100, 4)
    {
        for (ui32 node = 0; node < Runtime.GetNodeCount(); ++node) {
            auto& app = Runtime.GetAppData(node);
            app.FeatureFlags.SetEnableSnapshotsLocking(true);
            app.LongTxServiceConfig.SetMaxRemoteSnapshots(1);
            app.LongTxServiceConfig.SetLocalSnapshotPromotionTimeSeconds(0);
            app.LongTxServiceConfig.SetUnavailableNodeSnapshotsExpirationTimeSeconds(20);
        }
        Runtime.SimulateSleep(TDuration::Seconds(1));
    }

    static TWireSnapshots MakeRound(const TRowVersion& border, const TVector<TRemoteSnapshotInfo>& snapshots,
        const THashMap<ui32, TInstant>& nodes, const THashMap<ui32, ui64>& cutoffs = {})
    {
        TWireSnapshots result;
        result.SetBorderStep(border.Step);
        result.SetBorderTxId(border.TxId);
        for (const auto& snapshot : snapshots) {
            auto* item = result.AddSnapshots();
            item->SetSnapshotStep(snapshot.Snapshot.Step);
            item->SetSnapshotTxId(snapshot.Snapshot.TxId);
            ActorIdToProto(snapshot.SessionActorId, item->MutableSessionActorId());
            for (const auto& table : snapshot.TableIds) {
                auto* id = item->AddTableIds();
                id->SetOwnerId(table.PathId.OwnerId);
                id->SetTableId(table.PathId.LocalPathId);
            }
        }
        for (const auto& [node, time] : nodes) {
            auto* info = result.AddNodesCollectionInfo();
            info->SetNodeId(node);
            info->SetUpdateTime(time.Seconds());
            if (const auto* cutoff = cutoffs.FindPtr(node)) {
                info->SetPromotionCutoffStep(*cutoff);
            }
        }
        return result;
    }

    void ApplyRound(const TActorId& target, ui32 node, const TWireSnapshots& snapshots) {
        const auto sender = Runtime.AllocateEdgeActor(node);
        auto* request = new TEvLongTxService::TEvPropagateSnapshots();
        *request->Record.MutableSnapshots() = snapshots;
        Runtime.Send(new IEventHandle(target, sender, request), node, true);
        Runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvPropagateSnapshotsResult>(sender, TDuration::Seconds(3));
    }

    TActorId StartNode(ui32 node, const TRemoteSnapshotsStoragePtr& remote) {
        const auto actor = Runtime.RunCall([&] {
            auto local = MakeIntrusive<TLocalSnapshotsStorage>();
            const auto actor = Runtime.Register(CreateSnapshotExchangeActor(local, remote, {}), node);
            Runtime.EnableScheduleForActor(actor);
            return actor;
        });
        Runtime.WaitFor("new exchanger prefilled", [&] { return remote->IsReady(); }, TDuration::Seconds(3));
        return actor;
    }

    void CheckProtected(const TRemoteSnapshotsStoragePtr& storage, const TRowVersion& oldest,
        const TRowVersion& border, const TTableId& table)
    {
        UNIT_ASSERT_VALUES_EQUAL(storage->GetBorder(), border);
        auto builder = CreateImmutableSnapshotRegistryBuilder();
        builder->SetSnapshotBorder(storage->GetBorder());
        size_t count = 0;
        for (const auto& snapshot : storage->View()) {
            UNIT_ASSERT_VALUES_EQUAL(snapshot.Snapshot, oldest);
            builder->AddSnapshot(snapshot.TableIds, snapshot.Snapshot);
            ++count;
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 1u);
        const auto registry = std::move(*builder).Build();
        UNIT_ASSERT(registry->GetActiveSnapshots(table).contains(oldest));
        UNIT_ASSERT(!registry->GetActiveSnapshots(table).contains(border));
        // The omitted snapshot is protected by the border, including tables
        // for which the cache has no explicit points at all.
        UNIT_ASSERT(registry->HasSnapshot(table, border));
        UNIT_ASSERT(registry->HasSnapshot(TTableId(0, 2), border));
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(SnapshotExchangeSubscription) {

    Y_UNIT_TEST(UnreadyDonorDoesNotMakeJoiningNodeReadyAfterPrefillTimeout) {
        TSnapshotOverflowTest test;
        auto& runtime = test.Runtime;
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).LongTxServiceConfig.SetPrefillTimeoutSeconds(1);
        }
        // Neither startup nor periodic collection may supply an authoritative
        // cluster image before the explicit recovery propagation below.
        auto blockedCollection = runtime.AddObserver<TEvLongTxService::TEvCollectSnapshots>([](auto& ev) {
            ev.Reset();
        });
        const auto unavailablePublisher = test.PublishPeer();
        const auto donorStarted = runtime.GetCurrentTime();
        test.Start();
        runtime.WaitFor("donor requests its own prefill", [&] { return test.PrefillRequests == 1; }, TDuration::Seconds(3));
        UNIT_ASSERT(!test.RemoteStorage->IsReady());
        runtime.SimulateSleep(TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(test.LocalPublishRequests, 0u);
        runtime.WaitFor("donor publishes after its prefill timeout", [&] { return test.LocalPublishRequests > 0; },
            TDuration::Seconds(3));
        UNIT_ASSERT(runtime.GetCurrentTime() >= donorStarted + TDuration::Seconds(1));
        UNIT_ASSERT(!test.RemoteStorage->IsReady());

        // The unavailable original source leaves the board. The joining node
        // must now select the timed-out, published donor as its only peer.
        runtime.Send(new IEventHandle(unavailablePublisher, {}, new TEvents::TEvPoison()), 1, true);
        runtime.SimulateSleep(TDuration::Seconds(1));
        auto joiningStorage = MakeIntrusive<TRemoteSnapshotsStorage>();
        size_t joiningRequests = 0;
        size_t donorReplies = 0;
        size_t joiningPublications = 0;
        auto captureRequest = runtime.AddObserver<TEvLongTxService::TEvRemoteSnapshotsPrefill>([&](auto& ev) {
            if (ev->Sender.NodeId() == runtime.GetNodeId(2)) {
                UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite(), test.Exchanger);
                ++joiningRequests;
            }
        });
        auto captureReply = runtime.AddObserver<TEvLongTxService::TEvRemoteSnapshotsPrefillResult>([&](auto& ev) {
            if (ev->Sender == test.Exchanger && ev->GetRecipientRewrite().NodeId() == runtime.GetNodeId(2)) {
                ++donorReplies;
            }
        });
        auto capturePublish = runtime.AddObserver<TEvStateStorage::TEvReplicaBoardPublish>([&](auto& ev) {
            if (ev->Sender.NodeId() == runtime.GetNodeId(2)) {
                ++joiningPublications;
            }
        });
        const auto joiningStarted = runtime.GetCurrentTime();
        const auto joining = runtime.RunCall([&] {
            auto local = MakeIntrusive<TLocalSnapshotsStorage>();
            const auto actor = runtime.Register(CreateSnapshotExchangeActor(local, joiningStorage, {}), 2);
            runtime.EnableScheduleForActor(actor);
            return actor;
        });
        runtime.WaitFor("joining node requests prefill from unready donor", [&] { return joiningRequests == 1; },
            TDuration::Seconds(3));
        runtime.SimulateSleep(TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(donorReplies, 0u);
        UNIT_ASSERT_VALUES_EQUAL(joiningPublications, 0u);
        UNIT_ASSERT(!joiningStorage->IsReady());
        runtime.WaitFor("joining node publishes after its own prefill timeout", [&] { return joiningPublications > 0; },
            TDuration::Seconds(3));
        UNIT_ASSERT(runtime.GetCurrentTime() >= joiningStarted + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(donorReplies, 0u);
        UNIT_ASSERT(!test.RemoteStorage->IsReady());
        UNIT_ASSERT(!joiningStorage->IsReady());
        captureRequest.Remove();
        captureReply.Remove();
        capturePublish.Remove();

        const TTableId table(0, 1);
        const TRowVersion snapshot(100, 1);
        const TRowVersion border(1000, 1);
        const auto recovered = TSnapshotOverflowTest::MakeRound(border,
            {TRemoteSnapshotInfo(snapshot, test.Peer, {table})},
            {{test.Peer.NodeId(), runtime.GetCurrentTime()}});
        test.ApplyRound(test.Exchanger, 0, recovered);
        test.ApplyRound(joining, 2, recovered);
        UNIT_ASSERT(test.RemoteStorage->IsReady());
        UNIT_ASSERT(joiningStorage->IsReady());
        test.CheckProtected(test.RemoteStorage, snapshot, border, table);
        test.CheckProtected(joiningStorage, snapshot, border, table);

        // Once the donor has a real image, a subsequent prefill request works.
        const auto requester = runtime.AllocateEdgeActor(3);
        runtime.Send(new IEventHandle(test.Exchanger, requester, new TEvLongTxService::TEvRemoteSnapshotsPrefill()), 3, true);
        const auto reply = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvRemoteSnapshotsPrefillResult>(requester,
            TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(reply->Sender, test.Exchanger);
        const auto& snapshots = reply->Get()->Record.GetSnapshots();
        UNIT_ASSERT_VALUES_EQUAL(snapshots.SnapshotsSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(snapshots.GetSnapshots(0).GetSnapshotStep(), snapshot.Step);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion(snapshots.GetBorderStep(), snapshots.GetBorderTxId()), border);
    }

    Y_UNIT_TEST_QUAD(OverflowBorderSurvivesNonOwnerPrefillAndPartialRounds, EmptyOwner, Expire) {
        TSnapshotOverflowTest test;
        auto& runtime = test.Runtime;
        // ProceedToPublish starts a round immediately, independently of the
        // configured interval. Drive only the explicit rounds below.
        auto automaticCollections = runtime.AddObserver<TEvLongTxService::TEvCollectSnapshots>([](auto& ev) {
            if (ev->Sender == ev->GetRecipientRewrite()) {
                ev.Reset();
            }
        });
        const TTableId table(0, 1);
        const TRowVersion oldest(100, 1);
        const TRowVersion omitted(200, 1);
        const auto owner = runtime.AllocateEdgeActor(0);
        test.LocalStorage->Insert(TLocalSnapshotInfo(oldest, owner, {table}));
        if constexpr (!EmptyOwner) {
            test.LocalStorage->Insert(TLocalSnapshotInfo(omitted, owner, {table}));
        }
        test.StartHealthy();

        // Exercise the real collector's cap. In EmptyOwner, the second owner's
        // entire contribution is clipped, leaving only its collection metadata.
        const auto collectorSender = runtime.AllocateEdgeActor(0);
        auto* request = new TEvLongTxService::TEvCollectSnapshots();
        if constexpr (EmptyOwner) {
            ActorIdToProto(test.Peer, request->Record.MutableTree()->AddChildrenActorIds());
        }
        runtime.Send(new IEventHandle(test.Exchanger, collectorSender, request), 0, true);
        if constexpr (EmptyOwner) {
            const auto child = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvCollectSnapshots>(test.Peer);
            auto* reply = new TEvLongTxService::TEvCollectSnapshotsResult();
            *reply->Record.MutableSnapshots() = TSnapshotOverflowTest::MakeRound(TRowVersion::Max(),
                {TRemoteSnapshotInfo(omitted, test.Peer, {table})},
                {{test.Peer.NodeId(), runtime.GetCurrentTime()}});
            runtime.Send(new IEventHandle(child->Sender, test.Peer, reply), 1, true);
        }
        const auto collected = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvCollectSnapshotsResult>(collectorSender);
        const auto& initial = collected->Get()->Record.GetSnapshots();
        UNIT_ASSERT_VALUES_EQUAL(initial.SnapshotsSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(initial.NodesCollectionInfoSize(), EmptyOwner ? 2u : 1u);
        UNIT_ASSERT_VALUES_EQUAL(initial.GetSnapshots(0).GetSnapshotStep(), oldest.Step);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion(initial.GetBorderStep(), initial.GetBorderTxId()), omitted);
        test.ApplyRound(test.Exchanger, 0, initial);

        auto donorStorage = MakeIntrusive<TRemoteSnapshotsStorage>();
        const auto donor = test.StartNode(2, donorStorage);
        test.CheckProtected(donorStorage, oldest, omitted, table);
        const ui32 missingOwner = EmptyOwner ? test.Peer.NodeId() : owner.NodeId();
        UNIT_ASSERT(donorStorage->GetNodeIdToCollectionTime().contains(missingOwner));
        const auto originalCollection = donorStorage->GetNodeIdToCollectionTime().at(missingOwner);
        const TRemoteSnapshotInfo explicitSnapshot(oldest, owner, {table});

        // Collection times have seconds precision. An equal-time Max border
        // cannot supersede the conservative result already cached for that time.
        test.ApplyRound(donor, 2, TSnapshotOverflowTest::MakeRound(TRowVersion::Max(),
            {explicitSnapshot}, donorStorage->GetNodeIdToCollectionTime()));
        test.CheckProtected(donorStorage, oldest, omitted, table);

        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto partial = [&](bool includeDonor) {
            THashMap<ui32, TInstant> nodes{
                {EmptyOwner ? owner.NodeId() : test.Peer.NodeId(), runtime.GetCurrentTime()}};
            if (includeDonor) {
                nodes[donor.NodeId()] = runtime.GetCurrentTime();
            }
            return TSnapshotOverflowTest::MakeRound(TRowVersion::Max(),
                EmptyOwner ? TVector<TRemoteSnapshotInfo>{explicitSnapshot} : TVector<TRemoteSnapshotInfo>{}, nodes);
        };
        test.ApplyRound(donor, 2, partial(false));
        test.CheckProtected(donorStorage, oldest, omitted, table);
        UNIT_ASSERT_VALUES_EQUAL(donorStorage->GetNodeIdToCollectionTime().at(missingOwner), originalCollection);

        // Remove the original source from the board: the joining node can only
        // prefill from a non-owner donor that has A and the omitted-B border.
        runtime.Send(new IEventHandle(test.Exchanger, {}, new TEvents::TEvPoison()), 0, true);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!runtime.FindActor(test.Exchanger));
        bool nonOwnerPrefill = false;
        auto capturePrefill = runtime.AddObserver<TEvLongTxService::TEvRemoteSnapshotsPrefillResult>([&](auto& ev) {
            if (ev->GetRecipientRewrite().NodeId() == runtime.GetNodeId(3)) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Sender, donor);
                const auto& snapshots = ev->Get()->Record.GetSnapshots();
                UNIT_ASSERT_VALUES_EQUAL(snapshots.SnapshotsSize(), 1u);
                UNIT_ASSERT_VALUES_EQUAL(ActorIdFromProto(snapshots.GetSnapshots(0).GetSessionActorId()), owner);
                UNIT_ASSERT_VALUES_EQUAL(TRowVersion(snapshots.GetBorderStep(), snapshots.GetBorderTxId()), omitted);
                nonOwnerPrefill = true;
            }
        });
        auto joiningStorage = MakeIntrusive<TRemoteSnapshotsStorage>();
        const auto joining = test.StartNode(3, joiningStorage);
        capturePrefill.Remove();
        UNIT_ASSERT(nonOwnerPrefill);
        test.CheckProtected(joiningStorage, oldest, omitted, table);
        runtime.SimulateSleep(TDuration::Seconds(1));
        test.ApplyRound(joining, 3, partial(true));
        test.CheckProtected(joiningStorage, oldest, omitted, table);
        UNIT_ASSERT_VALUES_EQUAL(joiningStorage->GetNodeIdToCollectionTime().at(missingOwner), originalCollection);
        UNIT_ASSERT(runtime.GetCurrentTime() < originalCollection + TDuration::Seconds(20));

        if constexpr (Expire) {
            runtime.SimulateSleep(TDuration::Seconds(21));
            // An expired owner's delayed old report cannot renew its lifetime
            // or resurrect the previously cached omitted-snapshot border.
            test.ApplyRound(joining, 3, TSnapshotOverflowTest::MakeRound(TRowVersion::Max(), {},
                {{donor.NodeId(), runtime.GetCurrentTime()}, {missingOwner, originalCollection}}));
        } else {
            runtime.SimulateSleep(TDuration::Seconds(1));
            auto complete = joiningStorage->GetNodeIdToCollectionTime();
            for (auto& [node, collectionTime] : complete) {
                collectionTime = runtime.GetCurrentTime();
            }
            test.ApplyRound(joining, 3, TSnapshotOverflowTest::MakeRound(TRowVersion::Max(), {}, complete));
        }
        UNIT_ASSERT_VALUES_EQUAL(joiningStorage->GetBorder(), TRowVersion::Max());
        auto view = joiningStorage->View();
        UNIT_ASSERT(view.begin() == view.end());
        auto builder = CreateImmutableSnapshotRegistryBuilder();
        builder->SetSnapshotBorder(joiningStorage->GetBorder());
        const auto registry = std::move(*builder).Build();
        UNIT_ASSERT(!registry->HasSnapshot(table, oldest));
        UNIT_ASSERT(!registry->HasSnapshot(table, omitted));
    }

    Y_UNIT_TEST_QUAD(PromotionCutoffSurvivesNonOwnerPrefillAndPartialRounds, LegacyOwner, Expire) {
        TSnapshotOverflowTest test;
        auto& runtime = test.Runtime;
        auto automaticCollections = runtime.AddObserver<TEvLongTxService::TEvCollectSnapshots>([](auto& ev) {
            if (ev->Sender == ev->GetRecipientRewrite()) {
                ev.Reset();
            }
        });
        test.StartHealthy();
        const ui32 owner = test.Peer.NodeId();
        const auto originalCollection = runtime.GetCurrentTime();
        const ui64 initialCutoff = LegacyOwner ? 0 : 100;
        THashMap<ui32, ui64> initialCutoffs;
        if constexpr (!LegacyOwner) {
            initialCutoffs[owner] = initialCutoff;
        }
        // A young owner's entire contribution can consist of metadata: its
        // snapshots have not reached its local promotion cutoff yet.
        test.ApplyRound(test.Exchanger, 0, TSnapshotOverflowTest::MakeRound(TRowVersion::Max(), {},
            {{owner, originalCollection}}, initialCutoffs));
        UNIT_ASSERT_VALUES_EQUAL(test.RemoteStorage->GetPromotionCutoffStep(Max<ui64>()), initialCutoff);

        bool nonOwnerPrefill = false;
        auto capturePrefill = runtime.AddObserver<TEvLongTxService::TEvRemoteSnapshotsPrefillResult>([&](auto& ev) {
            if (ev->GetRecipientRewrite().NodeId() != runtime.GetNodeId(2)) {
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(ev->Sender, test.Exchanger);
            const auto& snapshots = ev->Get()->Record.GetSnapshots();
            UNIT_ASSERT_VALUES_EQUAL(snapshots.SnapshotsSize(), 0u);
            for (const auto& info : snapshots.GetNodesCollectionInfo()) {
                if (info.GetNodeId() == owner) {
                    UNIT_ASSERT_VALUES_EQUAL(info.GetUpdateTime(), originalCollection.Seconds());
                    UNIT_ASSERT_VALUES_EQUAL(info.GetPromotionCutoffStep(), initialCutoff);
                    nonOwnerPrefill = true;
                }
            }
        });
        auto storage = MakeIntrusive<TRemoteSnapshotsStorage>();
        const auto joining = test.StartNode(2, storage);
        capturePrefill.Remove();
        UNIT_ASSERT(nonOwnerPrefill);
        UNIT_ASSERT_VALUES_EQUAL(storage->GetNodeIdToPromotionCutoffStep().at(owner), initialCutoff);
        UNIT_ASSERT_VALUES_EQUAL(storage->GetPromotionCutoffStep(Max<ui64>()), initialCutoff);

        // Equal/older reports cannot raise a cached cutoff, even when an old
        // peer omitted the field and consequently established cutoff zero.
        test.ApplyRound(joining, 2, TSnapshotOverflowTest::MakeRound(TRowVersion::Max(), {},
            {{owner, originalCollection}}, {{owner, 1000}}));
        UNIT_ASSERT_VALUES_EQUAL(storage->GetPromotionCutoffStep(Max<ui64>()), initialCutoff);
        const ui64 loweredCutoff = LegacyOwner ? 0 : 50;
        test.ApplyRound(joining, 2, TSnapshotOverflowTest::MakeRound(TRowVersion::Max(), {},
            {{owner, originalCollection - TDuration::Seconds(1)}}, {{owner, loweredCutoff}}));
        UNIT_ASSERT_VALUES_EQUAL(storage->GetPromotionCutoffStep(Max<ui64>()), loweredCutoff);
        UNIT_ASSERT_VALUES_EQUAL(storage->GetNodeIdToCollectionTime().at(owner).Seconds(), originalCollection.Seconds());

        runtime.SimulateSleep(TDuration::Seconds(1));
        test.ApplyRound(joining, 2, TSnapshotOverflowTest::MakeRound(TRowVersion::Max(), {},
            {{owner, runtime.GetCurrentTime()}}, {{owner, 200}}));
        UNIT_ASSERT_VALUES_EQUAL(storage->GetPromotionCutoffStep(Max<ui64>()), 200u);
        runtime.SimulateSleep(TDuration::Seconds(1));
        const auto cachedCollection = runtime.GetCurrentTime();
        test.ApplyRound(joining, 2, TSnapshotOverflowTest::MakeRound(TRowVersion::Max(), {},
            {{owner, cachedCollection}}, {{owner, 150}}));
        UNIT_ASSERT_VALUES_EQUAL(storage->GetPromotionCutoffStep(Max<ui64>()), 150u);

        runtime.SimulateSleep(TDuration::Seconds(1));
        const ui32 donor = test.Exchanger.NodeId();
        test.ApplyRound(joining, 2, TSnapshotOverflowTest::MakeRound(TRowVersion::Max(), {},
            {{donor, runtime.GetCurrentTime()}}, {{donor, runtime.GetCurrentTime().MilliSeconds()}}));
        UNIT_ASSERT_VALUES_EQUAL(storage->GetNodeIdToCollectionTime().at(owner).Seconds(), cachedCollection.Seconds());
        UNIT_ASSERT_VALUES_EQUAL(storage->GetPromotionCutoffStep(Max<ui64>()), 150u);
        auto view = storage->View();
        UNIT_ASSERT(view.begin() == view.end());

        runtime.SimulateSleep(TDuration::Seconds(Expire ? 21 : 1));
        const auto freshCollection = runtime.GetCurrentTime();
        const ui64 freshCutoff = freshCollection.MilliSeconds();
        THashMap<ui32, TInstant> complete{{donor, freshCollection},
            {owner, Expire ? cachedCollection : freshCollection}};
        THashMap<ui32, ui64> completeCutoffs{{donor, freshCutoff}, {owner, Expire ? 150 : freshCutoff}};
        // A delayed replay cannot resurrect an expired owner's low cutoff.
        test.ApplyRound(joining, 2, TSnapshotOverflowTest::MakeRound(TRowVersion::Max(), {}, complete, completeCutoffs));
        UNIT_ASSERT_VALUES_EQUAL(storage->GetPromotionCutoffStep(Max<ui64>()), freshCutoff);
        UNIT_ASSERT_VALUES_EQUAL(storage->GetNodeIdToPromotionCutoffStep().contains(owner), !Expire);
        auto builder = CreateImmutableSnapshotRegistryBuilder();
        builder->SetMinPromotionCutoffStep(storage->GetPromotionCutoffStep(freshCutoff));
        const auto registry = std::move(*builder).Build();
        UNIT_ASSERT_VALUES_EQUAL(registry->GetMinPromotionCutoffStep(), freshCutoff);
    }

    Y_UNIT_TEST(HealthySubscriptionStartsImmediately) {
        TSnapshotSubscriptionTest test;
        const auto started = test.Runtime.GetCurrentTime();
        test.StartHealthy();
        UNIT_ASSERT(test.Runtime.GetCurrentTime() - started < TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(test.Subscribers.size(), 1);
        test.StopReplicas({2});
        test.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(test.Subscribers.size(), 1);
        UNIT_ASSERT(test.FailureTimes.empty());
    }

    Y_UNIT_TEST(InitialOrdinaryQuorumLossIsThrottledAndRecovers) {
        TSnapshotSubscriptionTest test;
        test.StopReplicas({1, 2});
        test.Start();
        test.WaitForFailure(1);
        test.CheckCooldown();
        UNIT_ASSERT_VALUES_EQUAL(test.LocalPublishRequests, 0);
        test.RestoreReplicas({1, 2});
        test.WaitForSuccess(1);
        UNIT_ASSERT_VALUES_EQUAL(test.Subscribers.size(), 2);
    }

    Y_UNIT_TEST(InitialBridgeGroupLossIsThrottledAndRecovers) {
        TSnapshotSubscriptionTest test(2);
        test.StopReplicas({3, 4, 5});
        test.Start();
        test.WaitForFailure(1);
        test.CheckCooldown();
        test.Runtime.SimulateSleep(TDuration::MilliSeconds(300));
        UNIT_ASSERT(test.Subscribers.size() <= 8);
        test.RestoreReplicas({3, 4, 5});
        test.WaitForSuccess(1);
        UNIT_ASSERT(test.Subscribers.size() <= 9);
    }

    Y_UNIT_TEST(ActiveOrdinaryQuorumLossPreservesSnapshots) {
        TSnapshotSubscriptionTest test;
        test.PublishPeer();
        test.Start();
        test.Runtime.WaitFor("prefill request", [&] { return test.PrefillRequests == 1; }, TDuration::Seconds(1));
        test.FinishPrefill();
        test.CheckPrefilledSnapshots();
        test.StopReplicas({1, 2});
        test.WaitForFailure(1);
        test.CheckCooldown();
        test.CheckPrefilledSnapshots();
        test.RestoreReplicas({1, 2});
        test.WaitForSuccess(2);
        test.CheckPrefilledSnapshots();
        UNIT_ASSERT_VALUES_EQUAL(test.PrefillRequests, 1);
    }

    Y_UNIT_TEST(ActiveBridgeGroupLossRecoversAfterFailoverDuringCooldown) {
        TSnapshotSubscriptionTest test(2);
        test.StartHealthy();
        test.StopReplicas({3, 4, 5});
        test.WaitForFailure(1);
        test.CheckCooldown();
        test.DisconnectSecondGroup();
        test.WaitForSuccess(2);
        const auto attempts = test.Subscribers.size();
        test.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(test.Subscribers.size(), attempts);
        UNIT_ASSERT_VALUES_EQUAL(test.FailureTimes.size(), 1);
    }

    Y_UNIT_TEST(RetryRunsWhileWaitingForPrefill) {
        TSnapshotSubscriptionTest test;
        test.PublishPeer();
        const auto localPublishRequests = test.LocalPublishRequests;
        test.Start();
        test.Runtime.WaitFor("prefill request", [&] { return test.PrefillRequests == 1; }, TDuration::Seconds(1));
        test.StopReplicas({1, 2});
        test.WaitForFailure(1);
        test.CheckCooldown();
        test.RestoreReplicas({1, 2});
        test.WaitForSuccess(2);
        UNIT_ASSERT_VALUES_EQUAL(test.LocalPublishRequests, localPublishRequests);
        test.FinishPrefill();
        test.CheckPrefilledSnapshots();
        UNIT_ASSERT_VALUES_EQUAL(test.PrefillRequests, 1);
    }

    Y_UNIT_TEST(PendingRetrySurvivesPrefillCompletion) {
        TSnapshotSubscriptionTest test;
        test.PublishPeer();
        test.Start();
        test.Runtime.WaitFor("prefill request", [&] { return test.PrefillRequests == 1; }, TDuration::Seconds(1));
        test.StopReplicas({1, 2});
        test.WaitForFailure(1);
        test.FinishPrefill();
        test.CheckPrefilledSnapshots();
        test.CheckCooldown();
        test.RestoreReplicas({1, 2});
        test.WaitForSuccess(2);
        test.CheckPrefilledSnapshots();
        UNIT_ASSERT_VALUES_EQUAL(test.PrefillRequests, 1);
    }

    Y_UNIT_TEST(DuplicateAndStaleResultsDoNotRestartSubscriptions) {
        TSnapshotSubscriptionTest test;
        test.Runtime.GetAppData().FeatureFlags.SetEnableSnapshotsLocking(true);
        test.Runtime.GetAppData().LongTxServiceConfig.SetSnapshotsExchangeIntervalSeconds(1);
        test.StopReplicas({1, 2});
        test.Start();
        test.WaitForFailure(1);
        const auto oldSubscriber = test.LastFailedSubscriber;
        for (size_t i = 0; i < 5; ++i) {
            test.SendBoardInfo(oldSubscriber, EBoardStatus::NotAvailable);
            test.SendBoardUpdate(oldSubscriber, EBoardStatus::NotAvailable);
            test.SendBoardInfo(oldSubscriber, EBoardStatus::Ok);
        }
        test.CheckCooldown();
        UNIT_ASSERT_VALUES_EQUAL(test.LocalPublishRequests, 0);
        test.RestoreReplicas({1, 2});
        test.WaitForSuccess(1);
        test.SendBoardInfo(oldSubscriber, EBoardStatus::NotAvailable);
        test.SendBoardUpdate(oldSubscriber, EBoardStatus::NotAvailable);
        test.SendBoardUpdate(oldSubscriber, EBoardStatus::Ok);
        test.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(test.Subscribers.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(test.CollectRequests, 0);
    }

    Y_UNIT_TEST(PoisonDuringCooldownDoesNotRespawnSubscriber) {
        TSnapshotSubscriptionTest test;
        test.StartHealthy();
        test.StopReplicas({1, 2});
        test.WaitForFailure(1);
        const auto attempts = test.Subscribers.size();
        test.Runtime.Send(new IEventHandle(test.Exchanger, {}, new TEvents::TEvPoison()), 0, true);
        test.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!test.Runtime.FindActor(test.Exchanger));
        UNIT_ASSERT_VALUES_EQUAL(test.Subscribers.size(), attempts);
    }

    Y_UNIT_TEST(HealthyBoardUpdatesContinueAfterRecovery) {
        TSnapshotSubscriptionTest test;
        auto& appData = test.Runtime.GetAppData();
        appData.LongTxServiceConfig.SetSnapshotsExchangeIntervalSeconds(1);
        appData.FeatureFlags.SetEnableSnapshotsLocking(true);
        test.StartHealthy();
        test.StopReplicas({1, 2});
        test.WaitForFailure(1);
        test.RestoreReplicas({1, 2});
        test.WaitForSuccess(2);
        UNIT_ASSERT(test.Peer < test.Exchanger);
        const auto publisher = test.PublishPeer();
        test.Runtime.SimulateSleep(TDuration::Seconds(1));
        auto collect = test.Runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvCollectSnapshots>(test.Peer, TDuration::Seconds(3));
        UNIT_ASSERT(collect);
        auto* collected = new TEvLongTxService::TEvCollectSnapshotsResult();
        collected->Record.MutableSnapshots()->SetBorderStep(Max<ui64>());
        collected->Record.MutableSnapshots()->SetBorderTxId(Max<ui64>());
        test.Runtime.Send(new IEventHandle(collect->Sender, test.Peer, collected), 1, true);
        auto propagate = test.Runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvPropagateSnapshots>(test.Peer, TDuration::Seconds(3));
        UNIT_ASSERT(propagate);
        test.Runtime.Send(new IEventHandle(propagate->Sender, test.Peer, new TEvLongTxService::TEvPropagateSnapshotsResult()), 1, true);
        test.Runtime.Send(new IEventHandle(publisher, {}, new TEvents::TEvPoisonPill()), 1, true);
        test.Runtime.SimulateSleep(TDuration::MilliSeconds(1100));
        UNIT_ASSERT_VALUES_EQUAL(test.CollectRequests, 1);
    }

    Y_UNIT_TEST(ExponentialBackoffIsCapped) {
        TSnapshotSubscriptionTest test(1, 100, 400);
        test.StopReplicas({1, 2});
        test.Start();
        test.WaitForFailure(7);
        for (size_t i = 1; i < test.FailureTimes.size(); ++i) {
            const auto nominal = TDuration::MilliSeconds(i == 1 ? 100 : i == 2 ? 200 : 400);
            const auto elapsed = test.FailureTimes[i] - test.FailureTimes[i - 1];
            UNIT_ASSERT_C(elapsed >= nominal / 2, "Retry did not back off: " << elapsed);
            UNIT_ASSERT_C(elapsed <= nominal, "Retry exceeded configured maximum: " << elapsed);
        }
        test.RestoreReplicas({1, 2});
        test.WaitForSuccess(1);
        test.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        test.StopReplicas({1, 2});
        test.WaitForFailure(9);
        const auto afterRecovery = test.FailureTimes.back() - test.FailureTimes[test.FailureTimes.size() - 2];
        UNIT_ASSERT(afterRecovery >= TDuration::MilliSeconds(50));
        UNIT_ASSERT(afterRecovery <= TDuration::MilliSeconds(100));
    }

    Y_UNIT_TEST(ZeroRetrySettingsHaveAPositiveDelay) {
        TSnapshotSubscriptionTest test(1, 0, 0);
        test.StopReplicas({1, 2});
        test.Start();
        test.WaitForFailure(5);
        for (size_t i = 1; i < test.FailureTimes.size(); ++i) {
            UNIT_ASSERT(test.FailureTimes[i] - test.FailureTimes[i - 1] >= TDuration::MilliSeconds(1));
        }
    }

    Y_UNIT_TEST(InvertedRetrySettingsHaveABoundedPositiveDelay) {
        TSnapshotSubscriptionTest test(1, 100, 10);
        test.StopReplicas({1, 2});
        test.Start();
        test.WaitForFailure(5);
        for (size_t i = 1; i < test.FailureTimes.size(); ++i) {
            const auto elapsed = test.FailureTimes[i] - test.FailureTimes[i - 1];
            UNIT_ASSERT(elapsed >= TDuration::MilliSeconds(50));
            UNIT_ASSERT(elapsed <= TDuration::MilliSeconds(100));
        }
    }
}

} // namespace NKikimr::NLongTxService
