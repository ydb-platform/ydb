#include <ydb/core/tx/long_tx_service/snapshots_exchange.h>

#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NLongTxService {
namespace {

using EBoardStatus = TEvStateStorage::TEvBoardInfo::EStatus;

class TSnapshotSubscriptionTest {
public:
    explicit TSnapshotSubscriptionTest(size_t groups = 1, ui32 startDelayMs = 100, ui32 maxDelayMs = 100)
        : Runtime(2)
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

} // anonymous namespace

Y_UNIT_TEST_SUITE(SnapshotExchangeSubscription) {

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
