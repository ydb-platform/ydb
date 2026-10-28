#include "events_schemeshard.h"
#include "populator.h"
#include "mon_events.h"
#include "two_part_description.h"
#include "ut_helpers.h"

#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/wait_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/services/services.pb.h>

#include <util/digest/city.h>
#include <util/generic/map.h>

namespace NKikimr {
namespace NSchemeBoard {

using namespace NSchemeShardUT_Private;

class TPopulatorTest: public TTestWithSchemeshard {
public:
    void SetUp() override {
        TTestWithSchemeshard::SetUp();
        Context->SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NLog::PRI_DEBUG);
    }

    UNIT_TEST_SUITE(TPopulatorTest);
    UNIT_TEST(Boot);
    UNIT_TEST(MakeDir);
    UNIT_TEST(RemoveDir);
    UNIT_TEST_SUITE_END();

    void Boot() {
        const TActorId edge = Context->AllocateEdgeActor();

        NKikimr::TPathId rootPathId(TTestTxConfig::SchemeShard, RootPathId);

        Context->CreateSubscriber(edge, rootPathId);
        auto ev = Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);

        UNIT_ASSERT(ev->Get());
        UNIT_ASSERT_VALUES_EQUAL("/Root", ev->Get()->Path);
        UNIT_ASSERT_VALUES_EQUAL(rootPathId, ev->Get()->PathId);
    }

    void MakeDir() {
        const TActorId edge = Context->AllocateEdgeActor();

        TestMkDir(*Context, 100, "/Root", "DirA");
        auto describe = DescribePath(*Context, "/Root/DirA");

        NKikimr::TPathId pathId(TTestTxConfig::SchemeShard, describe.GetPathId());

        Context->CreateSubscriber(edge, pathId);
        auto ev = Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);

        UNIT_ASSERT(ev->Get());
        UNIT_ASSERT_VALUES_EQUAL(describe.GetPath(), ev->Get()->Path);
        UNIT_ASSERT_VALUES_EQUAL(pathId, ev->Get()->PathId);
    }

    void RemoveDir() {
        const TActorId edge = Context->AllocateEdgeActor();

        TestMkDir(*Context, 100, "/Root", "DirB");
        auto describe = DescribePath(*Context, "/Root/DirB");

        NKikimr::TPathId pathId(TTestTxConfig::SchemeShard, describe.GetPathId());

        Context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyUpdate>(edge, pathId);
        TestRmDir(*Context, 101, "/Root", "DirB");
        auto ev = Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyDelete>(edge);

        UNIT_ASSERT(ev->Get());
        UNIT_ASSERT_VALUES_EQUAL(describe.GetPath(), ev->Get()->Path);
        UNIT_ASSERT_VALUES_EQUAL(pathId, ev->Get()->PathId);
    }

}; // TPopulatorTest

class TPopulatorTestWithResets: public TTestWithSchemeshard {
public:
    void SetUp() override {
        TTestWithSchemeshard::SetUp();
        Context->SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NLog::PRI_DEBUG);
    }

    TTestContext::TEventObserver ObserverFunc() override {
        return [this](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TSchemeBoardEvents::EvHandshakeRequest:
                ReplicaPopulators.emplace(ev->Sender, false);
                Context->EnableScheduleForActor(ev->Sender, true);
                break;

            case TSchemeBoardEvents::EvUpdateAck:
                auto it = ReplicaPopulators.find(ev->Recipient);
                if (DropFirstAcks && it != ReplicaPopulators.end() && !it->second) {
                    it->second = true;
                    Context->Send(ev->Recipient, ev->Sender, new TEvInterconnect::TEvNodeDisconnected(ev->Sender.NodeId()));

                    return TTestContext::EEventAction::DROP;
                }
                break;
            }

            return TTestContext::EEventAction::PROCESS;
        };
    }

    UNIT_TEST_SUITE(TPopulatorTestWithResets);
    UNIT_TEST(UpdateAck);
    UNIT_TEST_SUITE_END();

    void UpdateAck() {
        DropFirstAcks = true;
        TestMkDir(*Context, 100, "/Root", "DirC");
        TestWaitNotification(*Context, {100}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));
    }

private:
    TMap<TActorId, bool> ReplicaPopulators;
    bool DropFirstAcks = false;

}; // TPopulatorTestWithResets

UNIT_TEST_SUITE_REGISTRATION(TPopulatorTest);
UNIT_TEST_SUITE_REGISTRATION(TPopulatorTestWithResets);

Y_UNIT_TEST_SUITE(TPopulatorQuorumTest) {

    constexpr int ReplicasInRingGroup = 3;
    using TUpdateAck = NSchemeshardEvents::TEvUpdateAck;

    NKikimrScheme::TEvDescribeSchemeResult CreateSamplePathDescription(ui64 owner, const TPathId& pathId, const TString& path) {
        NKikimrScheme::TEvDescribeSchemeResult desc;

        desc.SetStatus(NKikimrScheme::StatusSuccess);
        desc.SetPathOwnerId(owner);
        desc.SetPathId(pathId.LocalPathId);
        desc.SetPath(path);

        return desc;
    }

    TVector<TUpdateAck::TPtr> GetAcksRequiredForQuorum(
        std::deque<TUpdateAck::TPtr>& blockedAcks,
        THashMap<TActorId, TActorId>& populatorToReplicaMap,
        const TVector<TStateStorageInfo::TRingGroup>& ringGroups
    ) {
        TVector<TUpdateAck::TPtr> requiredAcks;
        TVector<ui32> ringGroupAcks(ringGroups.size(), 0);

        THashMap<TActorId, size_t> replicaToRingGroupMap;
        for (size_t i = 0; i < ringGroups.size(); ++i) {
            const auto& ringGroup = ringGroups[i];
            for (const auto& ring : ringGroup.Rings) {
                for (const auto& replica : ring.Replicas) {
                    replicaToRingGroupMap[replica] = i;
                }
            }
        }

        for (const auto& event : blockedAcks) {
            const auto& replica = populatorToReplicaMap.at(event->Sender);
            const size_t ringGroup = replicaToRingGroupMap.at(replica);
            if (ShouldIgnoreInQuorum(ringGroups[ringGroup]) || IsMajorityReached(ringGroups[ringGroup], ringGroupAcks[ringGroup])) {
                // not required for quorum
                continue;
            }
            ringGroupAcks[ringGroup]++;
            requiredAcks.emplace_back(event);
        }

        return requiredAcks;
    }

    enum class EReconfiguration {
        None,
        Reorder,
        DuplicateAck,
    };

    void TestPopulatorQuorum(TVector<TStateStorageInfo::TRingGroup>&& ringGroupsConfiguration,
            EReconfiguration reconfiguration = EReconfiguration::None) {
        TTestBasicRuntime runtime;
        SetupMinimalRuntime(runtime, CreateCustomStateStorageSetupper(ringGroupsConfiguration, ReplicasInRingGroup));

        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NLog::PRI_DEBUG);

        const TActorId edge = runtime.AllocateEdgeActor();
        constexpr ui64 owner = TTestTxConfig::SchemeShard;

        auto stateStorageInfo = GetStateStorageInfo(runtime);
        TVector<TActorId> replicas;
        for (const auto& ringGroup : stateStorageInfo->RingGroups) {
            if (ShouldIgnore(ringGroup)) {
                continue;
            }
            for (const auto& ring : ringGroup.Rings) {
                for (const auto& replica : ring.Replicas) {
                    replicas.emplace_back(replica);
                }
            }
        }
        Cerr << "replicas: " << JoinSeq(", ", replicas) << '\n';

        THashMap<TActorId, TActorId> replicaActorToServiceMap;
        for (const auto& replica : replicas) {
            replicaActorToServiceMap[runtime.GetLocalServiceId(replica)] = replica;
        }
        {
            Cerr << "replicaActorToServiceMap:\n";
            for (const auto& [actor, service] : replicaActorToServiceMap) {
                Cerr << "\tactor: " << actor << ", service: " << service << '\n';

            }
        }
        THashMap<TActorId, TActorId> populatorToReplicaMap;
        using THandshake = NInternalEvents::TEvHandshakeResponse;
        auto handshakeObserver = runtime.AddObserver<THandshake>([&](THandshake::TPtr& ev) {
            auto* replicaService = replicaActorToServiceMap.FindPtr(ev->Sender);
            if (replicaService && FindPtr(replicas, *replicaService)) {
                populatorToReplicaMap[ev->Recipient] = *replicaService;
            }
        });
        TWaitForFirstEvent<TEvStateStorage::TEvListSchemeBoardResult> initializationWaiter(runtime);
        constexpr ui64 generation = 1;
        const TActorId populator = runtime.Register(
            CreateSchemeBoardPopulator(owner, generation, {}, 0)
        );
        initializationWaiter.Wait(TDuration::Seconds(10));
        initializationWaiter.Stop();

        const TPathId pathId(owner, 100);
        const TString path = "/Root/TestPath";
        auto describeResult = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
        describeResult->Record = CreateSamplePathDescription(owner, pathId, path);
        constexpr ui64 cookie = 12345;

        TBlockEvents<TUpdateAck> ackBlocker(runtime, [&](const TUpdateAck::TPtr& ev) {
            return ev->Recipient == populator && ev->Cookie == cookie;
        });
        runtime.Send(new IEventHandle(populator, edge, describeResult.Release(), 0, cookie));

        runtime.WaitFor("updates from replica populators", [&]() {
            return ackBlocker.size() == replicas.size();
        }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(populatorToReplicaMap.size(), replicas.size());
        {
            Cerr << "populatorToReplicaMap:\n";
            for (const auto& [populator, replica] : populatorToReplicaMap) {
                Cerr << "\tpopulator: " << populator << ", replica: " << replica << '\n';

            }
        }

        // Keep blocking ACKs from configuration-triggered retries as well.
        auto requiredAcks = GetAcksRequiredForQuorum(ackBlocker, populatorToReplicaMap, stateStorageInfo->RingGroups);
        UNIT_ASSERT(!requiredAcks.empty());
        if (reconfiguration == EReconfiguration::DuplicateAck) {
            const auto& first = requiredAcks.front();
            for (size_t i = 0; i < ReplicasInRingGroup; ++i) {
                auto ack = MakeHolder<TUpdateAck>(owner, generation, pathId, first->Get()->Record.GetVersion());
                runtime.Send(new IEventHandle(populator, first->Sender, ack.Release(), 0, cookie));
            }
            UNIT_ASSERT_VALUES_EQUAL(CountEvents<TUpdateAck>(runtime, false, edge), 0);
        }
        // resend all required acks except the last one
        for (int i = 0; i + 1 < ssize(requiredAcks); ++i) {
            runtime.Send(requiredAcks[i].Release());
        }
        UNIT_ASSERT_VALUES_EQUAL(CountEvents<TUpdateAck>(runtime, false, edge), 0);
        if (reconfiguration == EReconfiguration::Reorder) {
            auto updated = MakeIntrusive<TStateStorageInfo>();
            updated->RingGroups = stateStorageInfo->RingGroups;
            Reverse(updated->RingGroups.begin(), updated->RingGroups.end());
            runtime.Send(new IEventHandle(populator, edge, new TEvStateStorage::TEvListSchemeBoardResult(updated)));
            UNIT_ASSERT_VALUES_EQUAL(CountEvents<TUpdateAck>(runtime, false, edge), 0);
        }
        ackBlocker.Stop();
        runtime.Send(requiredAcks.back().Release());

        auto mainAck = runtime.GrabEdgeEvent<TUpdateAck>(edge, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(mainAck->Sender, populator, mainAck->ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(mainAck->Get()->GetPathId(), pathId, mainAck->ToString());
    }

    void TestPublicationAckDoesNotAdvanceFullSync(ui64 cookie) {
        TTestBasicRuntime runtime;
        SetupMinimalRuntime(runtime, CreateCustomStateStorageSetupper({{}}, ReplicasInRingGroup));
        const auto edge = runtime.AllocateEdgeActor();
        constexpr ui64 owner = TTestTxConfig::SchemeShard;
        const auto info = GetStateStorageInfo(runtime);
        const auto replica = runtime.GetLocalServiceId(info->RingGroups.front().Rings.front().Replicas.front());
        const TPathId batchEnd(owner, 100);
        TActorId child;
        auto handshakes = runtime.AddObserver<NInternalEvents::TEvHandshakeResponse>([&](auto& ev) {
            if (ev->Sender == replica) {
                child = ev->Recipient;
            }
        });
        // Delay this child's description request while a publication from
        // another sender reaches the parent first. Hold its later messages
        // too, preserving the child-to-parent FIFO order.
        TBlockEvents<NInternalEvents::TEvRequestDescribe> descriptionsPending(runtime, [&](const auto& ev) {
            return ev->Sender == child && ev->Get()->PathId.OwnerId == owner && ev->Get()->PathId.LocalPathId >= batchEnd.LocalPathId;
        });
        TBlockEvents<TUpdateAck> childAcks(runtime, [&](const TUpdateAck::TPtr& ev) {
            return ev->Sender == child;
        });
        size_t nextBatchRequests = 0;
        auto requests = runtime.AddObserver<NInternalEvents::TEvRequestDescribe>([&](auto& ev) {
            if (ev->Sender == child && ev->Get()->PathId == batchEnd.NextId()) {
                ++nextBatchRequests;
            }
        });
        std::vector<std::pair<TPathId, TTwoPartDescription>> descriptions;
        for (ui64 id = 1; id <= 101; ++id) {
            const TPathId pathId(owner, id);
            auto desc = GenerateDescribe(TStringBuilder() << "/Root/Path" << id, pathId, 1);
            desc.SetStatus(NKikimrScheme::StatusSuccess);
            descriptions.emplace_back(pathId, TTwoPartDescription("", std::move(desc)));
        }
        const auto populator = runtime.Register(CreateSchemeBoardPopulator(owner, 1, std::move(descriptions), 102));
        runtime.WaitFor("description of the last path in the batch", [&] { return descriptionsPending.size() == 1; }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(nextBatchRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(descriptionsPending.front()->Get()->PathId, batchEnd);
        const auto monEdge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(child, monEdge, new TSchemeBoardMonEvents::TEvInfoRequest(100)));
        auto state = runtime.GrabEdgeEvent<TSchemeBoardMonEvents::TEvInfoResponse>(monEdge);
        const auto& cursor = state->Get()->Record.GetReplicaPopulatorResponse().GetCurPathId();
        UNIT_ASSERT_VALUES_EQUAL(cursor.GetOwnerId(), owner);
        UNIT_ASSERT_VALUES_EQUAL(cursor.GetLocalPathId(), batchEnd.LocalPathId);
        bool publicationAckSeen = false;
        auto publicationAcks = runtime.AddObserver<TUpdateAck>([&](TUpdateAck::TPtr& ev) {
            if (ev->Recipient == child && ev->Get()->GetPathId() == batchEnd && ev->Get()->Record.GetVersion() == 2) {
                publicationAckSeen = true;
            }
        });
        auto desc = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
        desc->Record = GenerateDescribe("/Root/Path100", batchEnd, 2);
        desc->Record.SetStatus(NKikimrScheme::StatusSuccess);
        runtime.Send(new IEventHandle(populator, edge, desc.Release(), 0, cookie));
        auto ack = runtime.GrabEdgeEvent<TUpdateAck>(edge);
        UNIT_ASSERT_VALUES_EQUAL(ack->Cookie, cookie);
        runtime.WaitFor("live publication ACK at the synchronizing child", [&] { return publicationAckSeen; }, TDuration::Seconds(10));
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL_C(descriptionsPending.size(), 1,
            "a live update ACK must not request another description before the pending one returns");
        UNIT_ASSERT_VALUES_EQUAL_C(nextBatchRequests, 0,
            "a live update ACK must not advance full sync while that path is still being described");
        descriptionsPending.Stop().Unblock();
        childAcks.Stop().Unblock();
        runtime.WaitFor("next full-sync batch", [&] { return nextBatchRequests == 1; }, TDuration::Seconds(10));
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(nextBatchRequests, 1);
    }

    Y_UNIT_TEST(ZeroCookiePublicationAckDoesNotAdvanceFullSync) {
        TestPublicationAckDoesNotAdvanceFullSync(0);
    }

    Y_UNIT_TEST(NonzeroCookiePublicationAckDoesNotAdvanceFullSync) {
        TestPublicationAckDoesNotAdvanceFullSync(12345);
    }

    enum class EBacklogRecovery {
        Update,
        Delete,
        Reconnect,
    };

    void TestCompletedPublicationsDoNotAccumulate(bool offline, bool keepPending = true, ui64 pendingCookie = 10000,
            bool holdRefreshAck = false, EBacklogRecovery recovery = EBacklogRecovery::Update) {
        TTestBasicRuntime runtime;
        SetupMinimalRuntime(runtime, CreateCustomStateStorageSetupper({{}}, ReplicasInRingGroup));
        const auto edge = runtime.AllocateEdgeActor();
        const auto monEdge = runtime.AllocateEdgeActor();
        constexpr ui64 owner = TTestTxConfig::SchemeShard;
        const TPathId pathId(owner, 100);
        const auto info = GetStateStorageInfo(runtime);
        const auto replica = info->RingGroups.front().Rings.front().Replicas.front();
        const auto replicaActor = runtime.GetLocalServiceId(replica);

        // One replica cannot finish its handshake or return update ACKs;
        // the other two can still form a quorum.
        TActorId child;
        using THandshake = NInternalEvents::TEvHandshakeResponse;
        TBlockEvents<THandshake> handshakeBlocker(runtime, [&](const THandshake::TPtr& ev) {
            if (ev->Sender == replicaActor) {
                child = ev->Recipient;
                return offline;
            }
            return false;
        });
        const auto populator = runtime.Register(CreateSchemeBoardPopulator(owner, 1, {}, 0));
        runtime.WaitFor("replica handshake", [&] { return bool(child); }, TDuration::Seconds(10));
        TUpdateAck::TPtr lastReplicaAck;
        auto delayedReplicaAcks = runtime.AddObserver<TUpdateAck>([&](TUpdateAck::TPtr& ev) {
            if ((!offline || holdRefreshAck) && ev->Recipient == child) {
                // Retain only the last reply, so the test itself does not
                // accumulate the ACKs whose delivery it prevents.
                lastReplicaAck = std::move(ev);
            }
        });
        ui64 receivedVersion = 0;
        auto updateObserver = runtime.AddObserver<NInternalEvents::TEvUpdate>([&](NInternalEvents::TEvUpdate::TPtr& ev) {
            if (ev->Sender == child && ev->Get()->GetPathId() == pathId) {
                const auto& record = ev->Get()->GetRecord();
                receivedVersion = Max(receivedVersion, record.GetIsDeletion() ? Max<ui64>() : NSchemeBoard::GetPathVersion(record));
            }
        });

        auto publish = [&](ui64 version, ui64 cookie) {
            auto desc = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
            desc->Record = GenerateDescribe("/Root/TestPath", pathId, version);
            desc->Record.SetStatus(version == Max<ui64>() ? NKikimrScheme::StatusPathDoesNotExist : NKikimrScheme::StatusSuccess);
            runtime.Send(new IEventHandle(populator, edge, desc.Release(), 0, cookie));
        };
        auto retainedPairs = [&](bool expectPendingPath = true) {
            runtime.Send(new IEventHandle(child, monEdge, new TSchemeBoardMonEvents::TEvInfoRequest(100)));
            auto ev = runtime.GrabEdgeEvent<TSchemeBoardMonEvents::TEvInfoResponse>(monEdge);
            const auto& response = ev->Get()->Record.GetReplicaPopulatorResponse();
            const size_t paths = response.UpdatesSize() + response.UpdatesRequestedSize() + response.UpdatesInFlightSize();
            UNIT_ASSERT_VALUES_EQUAL(paths, expectPendingPath ? 1 : 0);
            size_t count = 0;
            for (const auto* updates : {&response.GetUpdates(), &response.GetUpdatesRequested(), &response.GetUpdatesInFlight()}) {
                for (const auto& update : *updates) {
                    for (const auto& version : update.GetVersions()) {
                        count += version.TxIdsSize();
                    }
                }
            }
            return count;
        };

        // Keep a genuinely unfinished publication: its cookie must survive
        // completion of later versions of the same path.
        TBlockEvents<TUpdateAck> pendingAcks(runtime, [&](const TUpdateAck::TPtr& ev) {
            return ev->Recipient == populator && ev->Cookie == pendingCookie && ev->Sender != child;
        });
        if (keepPending) {
            publish(1, pendingCookie);
            runtime.WaitFor("two pending publication ACKs", [&] { return pendingAcks.size() == 2; }, TDuration::Seconds(10));
        }

        constexpr ui64 publications = 512;
        ui64 latestVersion = recovery == EBacklogRecovery::Delete ? Max<ui64>() : publications + 1;
        size_t firstBatchPairs = 0;
        size_t finalPairs = 0;
        for (ui64 i = 1; i <= publications; ++i) {
            publish(i == publications ? latestVersion : i + 1, i);
            auto ack = runtime.GrabEdgeEvent<TUpdateAck>(edge);
            UNIT_ASSERT_VALUES_EQUAL(ack->Cookie, i);
            if (i == publications / 2) {
                firstBatchPairs = retainedPairs();
            }
        }
        finalPairs = retainedPairs();
        // Count the actual retained payload rather than process RSS, which
        // depends on allocator caching. Each pair is a separately allocated
        // tree node and contains two ui64 values plus tree/allocator overhead.
        Cerr << "retained pairs after 256/512 publications: " << firstBatchPairs << "/" << finalPairs
             << "; payload bytes: " << firstBatchPairs * 2 * sizeof(ui64)
             << "/" << finalPairs * 2 * sizeof(ui64) << Endl;

        // Resume delivery and verify both catch-up and the unfinished cookie.
        bool latestReplicaAckSeen = false;
        bool pendingAckFromRecoveredReplica = false;
        auto ackObserver = runtime.AddObserver<TUpdateAck>([&](TUpdateAck::TPtr& ev) {
            if (ev->Recipient == child && ev->Get()->Record.GetVersion() == latestVersion) {
                latestReplicaAckSeen = true;
            }
            if (ev->Sender == child && ev->Recipient == populator && ev->Cookie == pendingCookie) {
                pendingAckFromRecoveredReplica = true;
            }
        });
        if (offline) {
            handshakeBlocker.Stop().Unblock();
        }
        if (!offline || holdRefreshAck) {
            runtime.WaitFor("last replica ACK", [&] {
                return lastReplicaAck && lastReplicaAck->Get()->Record.GetVersion() == latestVersion;
            }, TDuration::Seconds(10));
            if (holdRefreshAck) {
                const auto count = retainedPairs();
                Cerr << "pairs while requested update ACK is held: " << count << Endl;
                UNIT_ASSERT_VALUES_EQUAL_C(count, keepPending ? 1 : 0,
                    "a requested update must not introduce a synthetic publication cookie");
            }
            if (recovery == EBacklogRecovery::Reconnect) {
                for (ui64 cycle = 1; cycle <= 5; ++cycle) {
                    runtime.Send(new IEventHandle(child, edge, new TEvInterconnect::TEvNodeDisconnected(replica.NodeId())));
                    ++latestVersion;
                    latestReplicaAckSeen = false;
                    publish(latestVersion, publications + cycle);
                    auto ack = runtime.GrabEdgeEvent<TUpdateAck>(edge);
                    UNIT_ASSERT_VALUES_EQUAL(ack->Cookie, publications + cycle);
                    runtime.Send(new IEventHandle(child, edge, new TEvents::TEvWakeup()));
                    runtime.WaitFor("requested update after reconnect", [&] {
                        return lastReplicaAck && lastReplicaAck->Get()->Record.GetVersion() == latestVersion;
                    }, TDuration::Seconds(10));
                    UNIT_ASSERT_VALUES_EQUAL_C(retainedPairs(), keepPending ? 1 : 0,
                        "reconnect " << cycle << " accumulated synthetic publication cookies");
                }
            }
            delayedReplicaAcks.Remove();
            runtime.Send(lastReplicaAck.Release(), 0, /* viaActorSystem */ true);
        }
        runtime.WaitFor("latest version and pending ACK from recovered replica", [&] {
            return receivedVersion == latestVersion && latestReplicaAckSeen && (!keepPending || pendingAckFromRecoveredReplica);
        }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(CountEvents<TUpdateAck>(runtime, false, edge), 0);
        if (keepPending) {
            pendingAcks.Unblock(1);
            auto ack = runtime.GrabEdgeEvent<TUpdateAck>(edge);
            UNIT_ASSERT_VALUES_EQUAL(ack->Cookie, pendingCookie);
            UNIT_ASSERT_VALUES_EQUAL(ack->Get()->GetPathId(), pathId);
        }
        UNIT_ASSERT_VALUES_EQUAL(retainedPairs(false), 0);

        const size_t expectedPairs = keepPending ? 1 : 0;
        UNIT_ASSERT_VALUES_EQUAL_C(firstBatchPairs, expectedPairs, "completed publications retained on lagging replica");
        UNIT_ASSERT_VALUES_EQUAL_C(finalPairs, expectedPairs, "memory grows with the number of completed publications");
    }

    Y_UNIT_TEST(CompletedPublicationsDoNotAccumulateOnOfflineReplica) {
        TestCompletedPublicationsDoNotAccumulate(true);
    }

    Y_UNIT_TEST(CompletedPublicationsDoNotAccumulateInFlight) {
        TestCompletedPublicationsDoNotAccumulate(false);
    }

    Y_UNIT_TEST(CompletedPublicationsLeaveEmptyPendingPath) {
        TestCompletedPublicationsDoNotAccumulate(true, false);
    }

    Y_UNIT_TEST(CompletedPublicationsKeepInFlightSlot) {
        TestCompletedPublicationsDoNotAccumulate(false, false);
    }

    Y_UNIT_TEST(RequestedUpdateDoesNotCreatePublicationCookie) {
        TestCompletedPublicationsDoNotAccumulate(true, false, 10000, true);
    }

    Y_UNIT_TEST(UnfinishedPublicationWithZeroCookieIsPreserved) {
        TestCompletedPublicationsDoNotAccumulate(true, true, 0);
    }

    Y_UNIT_TEST(CompletedDeletionIsDeliveredToRecoveredReplica) {
        TestCompletedPublicationsDoNotAccumulate(true, true, 10000, true, EBacklogRecovery::Delete);
    }

    Y_UNIT_TEST(ReconnectsWithoutAcksDoNotAccumulatePublicationCookies) {
        TestCompletedPublicationsDoNotAccumulate(true, true, 10000, true, EBacklogRecovery::Reconnect);
    }

    Y_UNIT_TEST(CompletedPublicationIsReleasedAfterRecipientReplacement) {
        TTestBasicRuntime runtime;
        SetupMinimalRuntime(runtime, CreateCustomStateStorageSetupper({{}}, 5));
        const auto edge = runtime.AllocateEdgeActor();
        const auto monEdge = runtime.AllocateEdgeActor();
        constexpr ui64 owner = TTestTxConfig::SchemeShard;
        constexpr ui64 cookie = 12345;
        const TPathId pathId(owner, 100);
        const auto info = GetStateStorageInfo(runtime);
        TVector<TActorId> replicas;
        for (const auto& ring : info->RingGroups.front().Rings) {
            for (const auto& replica : ring.Replicas) {
                replicas.push_back(replica);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(replicas.size(), 5);
        const auto removedReplica = runtime.GetLocalServiceId(replicas[0]);
        const auto laggingReplica = runtime.GetLocalServiceId(replicas[1]);
        TActorId removedChild;
        TActorId laggingChild;
        using THandshake = NInternalEvents::TEvHandshakeResponse;
        TBlockEvents<THandshake> handshakes(runtime, [&](const THandshake::TPtr& ev) {
            if (ev->Sender == removedReplica) {
                removedChild = ev->Recipient;
                return true;
            }
            if (ev->Sender == laggingReplica) {
                laggingChild = ev->Recipient;
                return true;
            }
            return false;
        });
        const auto populator = runtime.Register(CreateSchemeBoardPopulator(owner, 1, {}, 0));
        runtime.WaitFor("both lagging replica handshakes", [&] {
            return removedChild && laggingChild;
        }, TDuration::Seconds(10));
        TBlockEvents<TUpdateAck> acks(runtime, [&](const TUpdateAck::TPtr& ev) {
            return ev->Recipient == populator && ev->Cookie == cookie;
        });
        auto desc = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
        desc->Record = GenerateDescribe("/Root/TestPath", pathId, 1);
        desc->Record.SetStatus(NKikimrScheme::StatusSuccess);
        runtime.Send(new IEventHandle(populator, edge, desc.Release(), 0, cookie));
        runtime.WaitFor("three healthy recipients", [&] { return acks.size() == 3; }, TDuration::Seconds(10));

        auto inspect = [&](const TActorId& actor) {
            runtime.Send(new IEventHandle(actor, monEdge, new TSchemeBoardMonEvents::TEvInfoRequest(100)));
            return runtime.GrabEdgeEvent<TSchemeBoardMonEvents::TEvInfoResponse>(monEdge);
        };
        auto checkLagging = [&](size_t paths, size_t pairs) {
            auto ev = inspect(laggingChild);
            const auto& r = ev->Get()->Record.GetReplicaPopulatorResponse();
            UNIT_ASSERT_VALUES_EQUAL(r.UpdatesSize() + r.UpdatesRequestedSize() + r.UpdatesInFlightSize(), paths);
            size_t count = 0;
            for (const auto* updates : {&r.GetUpdates(), &r.GetUpdatesRequested(), &r.GetUpdatesInFlight()}) {
                for (const auto& update : *updates) {
                    for (const auto& version : update.GetVersions()) {
                        count += version.TxIdsSize();
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(count, pairs);
        };
        checkLagging(1, 1);

        // Replace one original recipient before the healthy majority ACKs.
        // The replacement does not reply, so the original three ACKs must
        // finish publication and release the other lagging recipient's pair.
        auto updated = MakeIntrusive<TStateStorageInfo>();
        updated->RingGroups = info->RingGroups;
        const auto replacement = runtime.AllocateEdgeActor();
        for (auto& ring : updated->RingGroups.front().Rings) {
            for (auto& replica : ring.Replicas) {
                if (replica == replicas[0]) {
                    replica = replacement;
                }
            }
        }
        runtime.Send(new IEventHandle(populator, edge, new TEvStateStorage::TEvListSchemeBoardResult(updated)));
        auto before = inspect(populator);
        const auto& parentBefore = before->Get()->Record.GetPopulatorResponse();
        UNIT_ASSERT_VALUES_EQUAL(parentBefore.ReplicaPopulatorsSize(), 5);
        UNIT_ASSERT_VALUES_EQUAL(parentBefore.UpdateAcksSize(), 1);
        for (const auto& actor : parentBefore.GetReplicaPopulators()) {
            UNIT_ASSERT(ActorIdFromProto(actor) != removedChild);
        }
        acks.Stop().Unblock();
        auto ack = runtime.GrabEdgeEvent<TUpdateAck>(edge, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(ack->Cookie, cookie);
        checkLagging(1, 0);
        UNIT_ASSERT_VALUES_EQUAL(inspect(populator)->Get()->Record.GetPopulatorResponse().UpdateAcksSize(), 0);

        bool caughtUp = false;
        auto observer = runtime.AddObserver<TUpdateAck>([&](TUpdateAck::TPtr& ev) {
            if (ev->Recipient == laggingChild && ev->Get()->GetPathId() == pathId && ev->Get()->Record.GetVersion() == 1) {
                caughtUp = true;
            }
        });
        handshakes.Stop().Unblock();
        runtime.WaitFor("remaining lagging replica catches up", [&] { return caughtUp; }, TDuration::Seconds(10));
        checkLagging(0, 0);
        UNIT_ASSERT_VALUES_EQUAL(CountEvents<TUpdateAck>(runtime, false, edge), 0);
    }

    void TestPublicationSurvivesReplicaReplacement(bool deletion, bool deleteAfterPublication = false, bool checkReplayCleanup = false) {
        TTestBasicRuntime runtime;
        SetupMinimalRuntime(runtime, CreateCustomStateStorageSetupper({{}, {}}, ReplicasInRingGroup));
        const auto edge = runtime.AllocateEdgeActor();
        const auto monEdge = runtime.AllocateEdgeActor();
        const auto info = GetStateStorageInfo(runtime);
        auto original = MakeIntrusive<TStateStorageInfo>();
        original->RingGroups.push_back(info->RingGroups[0]);
        auto replacement = MakeIntrusive<TStateStorageInfo>();
        replacement->RingGroups.push_back(info->RingGroups[1]);
        THashSet<TActorId> newReplicaActors;
        for (const auto& ring : replacement->RingGroups.front().Rings) {
            for (const auto& replica : ring.Replicas) {
                newReplicaActors.insert(runtime.GetLocalServiceId(replica));
            }
        }
        TActorId populator;
        auto configObserver = runtime.AddObserver<TEvStateStorage::TEvListSchemeBoardResult>([&](auto& ev) {
            if (ev->Recipient == populator) {
                ev->Get()->Info = original;
            }
        });
        constexpr ui64 owner = TTestTxConfig::SchemeShard;
        const TPathId pathId(owner, 100);
        TWaitForFirstEvent<TEvStateStorage::TEvListSchemeBoardResult> initialized(runtime);
        populator = runtime.Register(CreateSchemeBoardPopulator(owner, 1, {}, 0));
        initialized.Wait(TDuration::Seconds(10));
        initialized.Stop();
        auto publish = [&](ui64 cookie, bool remove, ui64 version = 1) {
            auto desc = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
            desc->Record = GenerateDescribe("/Root/TestPath", pathId, remove ? 2 : version);
            desc->Record.SetStatus(remove ? NKikimrScheme::StatusPathDoesNotExist : NKikimrScheme::StatusSuccess);
            runtime.Send(new IEventHandle(populator, edge, desc.Release(), 0, cookie));
        };
        if (deletion) {
            publish(10, false);
            auto ack = runtime.GrabEdgeEvent<TUpdateAck>(edge);
            UNIT_ASSERT_VALUES_EQUAL(ack->Cookie, 10);
        }
        constexpr ui64 cookie = 12345;
        TBlockEvents<TUpdateAck> oldAcks(runtime, [&](const TUpdateAck::TPtr& ev) {
            return ev->Recipient == populator && (ev->Cookie == cookie || ev->Cookie == cookie + 1);
        });
        publish(cookie, deletion);
        runtime.WaitFor("original publication ACKs", [&] { return oldAcks.size() == 3; }, TDuration::Seconds(10));
        if (deleteAfterPublication) {
            publish(cookie + 1, true);
            runtime.WaitFor("deletion ACKs", [&] { return oldAcks.size() == 6; }, TDuration::Seconds(10));
        }
        if (checkReplayCleanup) {
            // The old cookie is replayed using this newer description, so the
            // lagging child retains version 2 even for the version-1 publication.
            publish(cookie + 1, false, 2);
            runtime.WaitFor("newer publication ACKs", [&] { return oldAcks.size() == 6; }, TDuration::Seconds(10));
        }
        configObserver.Remove();
        TBlockEvents<NInternalEvents::TEvHandshakeResponse> newHandshakes(runtime, [&](const auto& ev) {
            return newReplicaActors.contains(ev->Sender);
        });
        runtime.Send(new IEventHandle(populator, edge, new TEvStateStorage::TEvListSchemeBoardResult(replacement)));
        runtime.WaitFor("replacement replica handshakes", [&] { return newHandshakes.size() == 3; }, TDuration::Seconds(10));
        oldAcks.Stop().Unblock();
        UNIT_ASSERT_VALUES_EQUAL_C(CountEvents<TUpdateAck>(runtime, false, edge), 0,
            "ACKs from removed replica incarnations must not complete the publication");
        size_t committed = 0;
        auto commits = runtime.AddObserver<NInternalEvents::TEvCommitResponse>([&](auto& ev) {
            if (newReplicaActors.contains(ev->Sender)) {
                ++committed;
            }
        });
        if (checkReplayCleanup) {
            newHandshakes.Unblock(2);
            runtime.WaitFor("replacement quorum synchronized", [&] { return committed == 2; }, TDuration::Seconds(10));
            // Consume the ACKs: observers can see an edge-mailbox event
            // repeatedly while it remains queued in the test runtime.
            THashSet<ui64> completedCookies;
            for (size_t i = 0; i < 2; ++i) {
                auto ack = runtime.GrabEdgeEvent<TUpdateAck>(edge, TDuration::Seconds(10));
                UNIT_ASSERT(ack->Cookie == cookie || ack->Cookie == cookie + 1);
                UNIT_ASSERT_VALUES_EQUAL(ack->Get()->GetPathId(), pathId);
                UNIT_ASSERT_VALUES_EQUAL(ack->Get()->Record.GetVersion(), ack->Cookie == cookie ? 1 : 2);
                UNIT_ASSERT(completedCookies.insert(ack->Cookie).second);
            }
            const auto laggingChild = newHandshakes.front()->Recipient;
            runtime.Send(new IEventHandle(laggingChild, monEdge, new TSchemeBoardMonEvents::TEvInfoRequest(100)));
            auto state = runtime.GrabEdgeEvent<TSchemeBoardMonEvents::TEvInfoResponse>(monEdge);
            const auto& child = state->Get()->Record.GetReplicaPopulatorResponse();
            UNIT_ASSERT_VALUES_EQUAL(child.UpdatesSize(), 1);
            for (const auto* updates : {&child.GetUpdates(), &child.GetUpdatesRequested(), &child.GetUpdatesInFlight()}) {
                for (const auto& update : *updates) {
                    UNIT_ASSERT_VALUES_EQUAL_C(update.VersionsSize(), 0,
                        "completed replays must not retain newer-version publication markers");
                }
            }
        }
        newHandshakes.Stop().Unblock();
        runtime.WaitFor("replacement replicas fully synchronized", [&] { return committed == 3; }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(CountEvents<TUpdateAck>(runtime, false, edge), checkReplayCleanup ? 0 : (deleteAfterPublication ? 2 : 1),
            "publication must finish on the new replica configuration without a client retry");
        runtime.Send(new IEventHandle(populator, monEdge, new TSchemeBoardMonEvents::TEvInfoRequest(100)));
        auto response = runtime.GrabEdgeEvent<TSchemeBoardMonEvents::TEvInfoResponse>(monEdge);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetPopulatorResponse().UpdateAcksSize(), 0);
    }

    Y_UNIT_TEST(PublicationSurvivesReplicaReplacement) {
        TestPublicationSurvivesReplicaReplacement(false);
    }

    Y_UNIT_TEST(DeletionSurvivesReplicaReplacement) {
        TestPublicationSurvivesReplicaReplacement(true);
    }

    Y_UNIT_TEST(UpdateThenDeletionSurvivesReplicaReplacement) {
        TestPublicationSurvivesReplicaReplacement(false, true);
    }

    Y_UNIT_TEST(CompletedReplaysReleaseNewerDescriptionMarkers) {
        TestPublicationSurvivesReplicaReplacement(false, false, true);
    }

    Y_UNIT_TEST(ReplayUsesCurrentDescriptionPath) {
        TTestBasicRuntime runtime;
        TStateStorageInfo::TRingGroup group;
        group.NToSelect = 3;
        constexpr size_t replicaCount = 7;
        SetupMinimalRuntime(runtime, CreateCustomStateStorageSetupper({group}, replicaCount));
        const auto edge = runtime.AllocateEdgeActor();
        const auto info = GetStateStorageInfo(runtime);
        constexpr ui64 owner = TTestTxConfig::SchemeShard;
        const TPathId pathId(owner, 100);
        THashMap<TActorId, TActorId> actorToService;
        for (const auto& ring : info->RingGroups.front().Rings) {
            for (const auto& replica : ring.Replicas) {
                actorToService[runtime.GetLocalServiceId(replica)] = replica;
            }
        }
        THashMap<TActorId, TActorId> childToService;
        auto handshakes = runtime.AddObserver<NInternalEvents::TEvHandshakeResponse>([&](auto& ev) {
            childToService[ev->Recipient] = actorToService.at(ev->Sender);
        });
        size_t committed = 0;
        auto commits = runtime.AddObserver<NInternalEvents::TEvCommitResponse>([&](auto&) { ++committed; });
        const auto populator = runtime.Register(CreateSchemeBoardPopulator(owner, 1, {}, 0));
        runtime.WaitFor("all replica children initialized", [&] { return committed == replicaCount; }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(childToService.size(), replicaCount);
        auto select = [&](const TString& path) {
            THashSet<TActorId> selected;
            TStateStorageInfo::TSelection selection;
            info->SelectReplicas(CityHash64(path), &selection, 0);
            selected.insert(selection.begin(), selection.end());
            info->SelectReplicas(pathId.Hash(), &selection, 0);
            selected.insert(selection.begin(), selection.end());
            return selected;
        };
        const TString oldPath = "/Root/OldPath";
        const auto oldSelection = select(oldPath);
        TString newPath;
        THashSet<TActorId> newSelection;
        for (size_t i = 0; i != 100; ++i) {
            newPath = TStringBuilder() << "/Root/NewPath" << i;
            newSelection = select(newPath);
            if (newSelection != oldSelection) {
                break;
            }
        }
        UNIT_ASSERT_C(newSelection != oldSelection, "test paths must select different replica sets");
        // Hold all publications on the parent-to-child links in their original
        // order. No ACK can complete the old publication before replay.
        TBlockEvents<NInternalEvents::TEvUpdate> updates(runtime, [&](const auto& ev) {
            return ev->Sender == populator;
        });
        auto publish = [&](const TString& path, ui64 version, ui64 cookie) {
            auto desc = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
            desc->Record = GenerateDescribe(path, pathId, version);
            desc->Record.SetStatus(NKikimrScheme::StatusSuccess);
            runtime.Send(new IEventHandle(populator, edge, desc.Release(), 0, cookie));
        };
        constexpr ui64 oldCookie = 12345;
        publish(oldPath, 1, oldCookie);
        runtime.WaitFor("old publication", [&] { return updates.size() == oldSelection.size(); }, TDuration::Seconds(10));
        publish(newPath, 2, oldCookie + 1);
        const auto originalCount = oldSelection.size() + newSelection.size();
        runtime.WaitFor("new publication", [&] { return updates.size() == originalCount; }, TDuration::Seconds(10));
        runtime.Send(new IEventHandle(populator, edge, new TEvStateStorage::TEvListSchemeBoardResult(info)));
        runtime.SimulateSleep(TDuration::Seconds(1));
        THashSet<TActorId> replayedTo;
        for (size_t i = originalCount; i < updates.size(); ++i) {
            const auto& ev = updates[i];
            if (ev->Cookie == oldCookie) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPath(), newPath);
                replayedTo.insert(childToService.at(ev->Recipient));
            }
        }
        UNIT_ASSERT_C(replayedTo == newSelection,
            "replay of the latest description must use that description's current path for routing");
    }

    void TestColdDeletionWithPathWaitsForReplicaQuorum(bool subscribeBeforeDeletion) {
        TTestBasicRuntime runtime;
        TStateStorageInfo::TRingGroup group{};
        group.NToSelect = ReplicasInRingGroup;
        SetupMinimalRuntime(runtime, CreateCustomStateStorageSetupper({group}, ReplicasInRingGroup));
        const auto edge = runtime.AllocateEdgeActor();
        constexpr ui64 owner = TTestTxConfig::SchemeShard;
        const TPathId pathId(owner, 100);
        const auto info = GetStateStorageInfo(runtime);
        THashSet<TActorId> replicaActors;
        for (const auto& ring : info->RingGroups.front().Rings) {
            for (const auto& replica : ring.Replicas) {
                replicaActors.insert(runtime.GetLocalServiceId(replica));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(replicaActors.size(), ReplicasInRingGroup);
        size_t committed = 0;
        auto commits = runtime.AddObserver<NInternalEvents::TEvCommitResponse>([&](auto& ev) {
            if (replicaActors.contains(ev->Sender)) {
                ++committed;
            }
        });
        const TActorId populator = runtime.Register(CreateSchemeBoardPopulator(owner, 1, {}, 0));
        runtime.WaitFor("all replicas initialized", [&] {
            return committed == ReplicasInRingGroup;
        }, TDuration::Seconds(10));
        commits.Remove();

        if (subscribeBeforeDeletion) {
            const auto subscriber = runtime.AllocateEdgeActor();
            for (const auto& replica : replicaActors) {
                runtime.Send(new IEventHandle(replica, subscriber, new NInternalEvents::TEvSubscribe(pathId, owner)));
                auto notify = runtime.GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriber, TDuration::Seconds(10));
                UNIT_ASSERT_VALUES_EQUAL(notify->Sender, replica);
                const auto& record = notify->Get()->GetRecord();
                UNIT_ASSERT_VALUES_EQUAL(TPathId(record.GetPathOwnerId(), record.GetLocalPathId()), pathId);
                UNIT_ASSERT(record.GetIsDeletion());
                UNIT_ASSERT_VALUES_EQUAL(record.GetVersion(), 0);
            }
        }

        constexpr ui64 cookie = 12345;
        TBlockEvents<NInternalEvents::TEvUpdate> updates(runtime, [&](const auto& ev) {
            return ev->Sender == populator && ev->Cookie == cookie;
        });
        TBlockEvents<TUpdateAck> replicaAcks(runtime, [&](const auto& ev) {
            // Replica updates use a child-local cookie, not the client's publication cookie.
            return replicaActors.contains(ev->Sender) && ev->Get()->GetPathId() == pathId;
        });
        TBlockEvents<TUpdateAck> mainAcks(runtime, [&](const auto& ev) {
            return ev->Recipient == edge && ev->Cookie == cookie;
        });

        auto deletion = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
        deletion->Record = GenerateDescribe("/Root/TestPath", pathId, 2);
        deletion->Record.SetStatus(NKikimrScheme::StatusPathDoesNotExist);
        runtime.Send(new IEventHandle(populator, edge, deletion.Release(), 0, cookie));
        runtime.WaitFor("cold deletion routed to replicas", [&] {
            return updates.size() == ReplicasInRingGroup || !mainAcks.empty();
        }, TDuration::Seconds(10));

        UNIT_ASSERT_C(mainAcks.empty(),
            "a deletion replay with a routing path must not be acknowledged before replica quorum");
        UNIT_ASSERT_VALUES_EQUAL(updates.size(), ReplicasInRingGroup);
        mainAcks.Stop();
        updates.Stop().Unblock();
        runtime.WaitFor("cold deletion replica ACKs", [&] {
            return replicaAcks.size() == ReplicasInRingGroup;
        }, TDuration::Seconds(10));
        for (const auto& ack : replicaAcks) {
            UNIT_ASSERT_VALUES_EQUAL(ack->Get()->GetPathId(), pathId);
            UNIT_ASSERT_VALUES_EQUAL_C(ack->Get()->Record.GetVersion(), Max<ui64>(), ack->ToString());
        }
        UNIT_ASSERT_VALUES_EQUAL(CountEvents<TUpdateAck>(runtime, false, edge), 0);

        replicaAcks.Unblock(1);
        UNIT_ASSERT_VALUES_EQUAL_C(CountEvents<TUpdateAck>(runtime, false, edge), 0,
            "one replica ACK must not complete a deletion requiring two of three replicas");
        replicaAcks.Unblock(1);
        auto mainAck = runtime.GrabEdgeEvent<TUpdateAck>(edge, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(mainAck->Sender, populator);
        UNIT_ASSERT_VALUES_EQUAL(mainAck->Cookie, cookie);
        UNIT_ASSERT_VALUES_EQUAL(mainAck->Get()->GetPathId(), pathId);
        UNIT_ASSERT_VALUES_EQUAL(mainAck->Get()->Record.GetVersion(), Max<ui64>());
    }

    Y_UNIT_TEST(ColdDeletionWithPathWaitsForReplicaQuorum) {
        TestColdDeletionWithPathWaitsForReplicaQuorum(false);
    }

    Y_UNIT_TEST(ColdDeletionWithPathAfterSubscriptionWaitsForReplicaQuorum) {
        TestColdDeletionWithPathWaitsForReplicaQuorum(true);
    }

    Y_UNIT_TEST(OneRingGroup) {
        TestPopulatorQuorum({ {} });
    }

    Y_UNIT_TEST(TwoRingGroups) {
        TestPopulatorQuorum({ {.State = PRIMARY}, {.State = SYNCHRONIZED} });
    }

    Y_UNIT_TEST(ReorderRingGroupsDuringPublication) {
        TestPopulatorQuorum({ {.State = PRIMARY}, {.State = SYNCHRONIZED} }, EReconfiguration::Reorder);
    }

    Y_UNIT_TEST(DuplicateReplicaAckDoesNotReachQuorum) {
        TestPopulatorQuorum({ {} }, EReconfiguration::DuplicateAck);
    }

    Y_UNIT_TEST(OneDisconnectedRingGroup) {
        TestPopulatorQuorum({ {.State = PRIMARY}, {.State = DISCONNECTED} });
    }

    Y_UNIT_TEST(OneWriteOnlyRingGroup) {
        TestPopulatorQuorum({ {.State = PRIMARY}, {.State = PRIMARY, .WriteOnly = true} });
    }
}

} // NSchemeBoard
} // NKikimr
