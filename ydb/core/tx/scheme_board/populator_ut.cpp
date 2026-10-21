#include "events_schemeshard.h"
#include "populator.h"
#include "mon_events.h"
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

    void TestPublicationSurvivesReplicaReplacement(bool deletion, bool deleteAfterPublication = false) {
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
        auto publish = [&](ui64 cookie, bool remove) {
            auto desc = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
            desc->Record = GenerateDescribe("/Root/TestPath", pathId, remove ? 2 : 1);
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
        newHandshakes.Stop().Unblock();
        runtime.WaitFor("replacement replicas fully synchronized", [&] { return committed == 3; }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(CountEvents<TUpdateAck>(runtime, false, edge), deleteAfterPublication ? 2 : 1,
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
