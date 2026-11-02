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

    void TestReplayUsesCurrentDescriptionPath(bool replaceReplicas) {
        TTestBasicRuntime runtime;
        TStateStorageInfo::TRingGroup group{};
        group.NToSelect = 3;
        constexpr size_t replicaCount = 7;
        SetupMinimalRuntime(runtime, CreateCustomStateStorageSetupper({group, group}, replicaCount));
        const auto edge = runtime.AllocateEdgeActor();
        const auto monEdge = runtime.AllocateEdgeActor();
        const auto info = GetStateStorageInfo(runtime);
        auto original = MakeIntrusive<TStateStorageInfo>();
        original->RingGroups.push_back(info->RingGroups[0]);
        auto replacement = MakeIntrusive<TStateStorageInfo>();
        replacement->RingGroups.push_back(info->RingGroups[1]);
        const auto current = replaceReplicas ? replacement : original;
        constexpr ui64 owner = TTestTxConfig::SchemeShard;
        const TPathId pathId(owner, 100);
        THashMap<TActorId, TActorId> actorToService;
        for (const auto& ringGroup : info->RingGroups) {
            for (const auto& ring : ringGroup.Rings) {
                for (const auto& replica : ring.Replicas) {
                    actorToService[runtime.GetLocalServiceId(replica)] = replica;
                }
            }
        }
        THashMap<TActorId, TActorId> childToService;
        auto handshakes = runtime.AddObserver<NInternalEvents::TEvHandshakeResponse>([&](auto& ev) {
            childToService[ev->Recipient] = actorToService.at(ev->Sender);
        });
        size_t committed = 0;
        auto commits = runtime.AddObserver<NInternalEvents::TEvCommitResponse>([&](auto&) { ++committed; });
        TActorId populator;
        auto configObserver = runtime.AddObserver<TEvStateStorage::TEvListSchemeBoardResult>([&](auto& ev) {
            if (ev->Recipient == populator) {
                ev->Get()->Info = original;
            }
        });
        populator = runtime.Register(CreateSchemeBoardPopulator(owner, 1, {}, 0));
        runtime.WaitFor("all original replica children initialized", [&] { return committed == replicaCount; }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(childToService.size(), replicaCount);
        configObserver.Remove();
        auto select = [&](const TStateStorageInfo& config, ui64 key) {
            TStateStorageInfo::TSelection selection;
            config.SelectReplicas(key, &selection, 0);
            return THashSet<TActorId>(selection.begin(), selection.end());
        };
        auto overlap = [](const auto& lhs, const auto& rhs) {
            size_t count = 0;
            for (const auto& replica : lhs) {
                count += rhs.contains(replica);
            }
            return count;
        };
        const auto idSelection = select(*current, GetSchemeBoardSelectionKey(pathId));
        TString newPath;
        THashSet<TActorId> newSelection;
        for (size_t i = 0; i != 1000; ++i) {
            newPath = TStringBuilder() << "/Root/NewPath" << i;
            newSelection = select(*current, GetSchemeBoardSelectionKey(newPath));
            if (newSelection == idSelection) {
                break;
            }
        }
        UNIT_ASSERT_C(newSelection == idSelection, "the new path and PathId must select the same replicas");
        TString oldPath;
        THashSet<TActorId> oldPathSelection;
        for (size_t i = 0; i != 1000; ++i) {
            oldPath = TStringBuilder() << "/Root/OldPath" << i;
            oldPathSelection = select(*current, GetSchemeBoardSelectionKey(oldPath));
            if (overlap(oldPathSelection, newSelection) < 2) {
                break;
            }
        }
        UNIT_ASSERT_C(overlap(oldPathSelection, newSelection) < 2,
            "all current destinations must be insufficient for the old path quorum");
        const auto originalIdSelection = select(*original, GetSchemeBoardSelectionKey(pathId));
        const auto originalOldPathSelection = select(*original, GetSchemeBoardSelectionKey(oldPath));
        UNIT_ASSERT(overlap(originalOldPathSelection, originalIdSelection) < 2);
        auto oldSelection = originalOldPathSelection;
        oldSelection.insert(originalIdSelection.begin(), originalIdSelection.end());
        const auto originalNewSelection = select(*original, GetSchemeBoardSelectionKey(newPath));
        UNIT_ASSERT(originalNewSelection == originalIdSelection);
        auto publish = [&](const TString& path, ui64 version, ui64 cookie) {
            auto desc = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
            desc->Record = GenerateDescribe(path, pathId, version);
            desc->Record.SetStatus(NKikimrScheme::StatusSuccess);
            runtime.Send(new IEventHandle(populator, edge, desc.Release(), 0, cookie));
        };
        constexpr ui64 oldCookie = 12345;
        THashMap<ui64, ui64> completed;
        auto completions = runtime.AddObserver<TUpdateAck>([&](auto& ev) {
            if (ev->Recipient == edge) {
                UNIT_ASSERT_C(completed.emplace(ev->Cookie, ev->Get()->Record.GetVersion()).second,
                    "a publication must complete only once");
                ev.Reset();
            }
        });
        TBlockEvents<TUpdateAck> oldAcks(runtime, [&](const auto& ev) {
            return ev->Recipient == populator && ev->Cookie == oldCookie;
        });
        publish(oldPath, 1, oldCookie);
        runtime.WaitFor("original publication ACKs", [&] { return oldAcks.size() == oldSelection.size(); }, TDuration::Seconds(10));
        oldAcks.Stop();
        TVector<TUpdateAck::TPtr> oldIdAcks;
        for (const auto& ev : oldAcks) {
            if (originalIdSelection.contains(childToService.at(ev->Sender))) {
                oldIdAcks.emplace_back(ev);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(oldIdAcks.size(), 3);
        // This is a quorum for the new route, but not for the old path.
        // Keep one more old-version ACK in flight across replay.
        runtime.Send(oldIdAcks[0].Release(), 0, /* viaActorSystem */ true);
        runtime.Send(oldIdAcks[1].Release(), 0, /* viaActorSystem */ true);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(completed.empty());
        TBlockEvents<NInternalEvents::TEvUpdate> updates(runtime, [&](const auto& ev) {
            return ev->Sender == populator;
        });
        publish(newPath, 2, oldCookie + 1);
        runtime.WaitFor("new publication", [&] { return updates.size() == originalNewSelection.size(); }, TDuration::Seconds(10));
        runtime.Send(new IEventHandle(populator, edge, new TEvStateStorage::TEvListSchemeBoardResult(current)));
        const auto expectedCommits = replaceReplicas ? 2 * replicaCount : replicaCount;
        runtime.WaitFor("replay to the current replica children", [&] {
            return committed == expectedCommits && updates.size() == originalNewSelection.size() + 2 * newSelection.size();
        }, TDuration::Seconds(10));
        const auto delayedAckSender = oldIdAcks[2]->Sender;
        THashMap<ui64, THashSet<TActorId>> replayedTo;
        size_t freshAckIndex = updates.size();
        for (size_t i = 0; i < updates.size(); ++i) {
            const auto& ev = updates[i];
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPath(), newPath);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPathId(), pathId);
            UNIT_ASSERT_VALUES_EQUAL(NSchemeBoard::GetPathVersion(ev->Get()->GetRecord()), 2);
            if (i < originalNewSelection.size()) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, oldCookie + 1);
                UNIT_ASSERT(originalNewSelection.contains(childToService.at(ev->Recipient)));
            } else {
                UNIT_ASSERT(ev->Cookie == oldCookie || ev->Cookie == oldCookie + 1);
                replayedTo[ev->Cookie].insert(childToService.at(ev->Recipient));
                if (ev->Cookie == oldCookie && ev->Recipient != delayedAckSender && freshAckIndex == updates.size()) {
                    freshAckIndex = i;
                }
            }
        }
        UNIT_ASSERT_C(replayedTo.at(oldCookie) == newSelection && replayedTo.at(oldCookie + 1) == newSelection,
            "replay of the latest description must use that description's current path for routing");
        runtime.Send(oldIdAcks[2].Release(), 0, /* viaActorSystem */ true);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_C(!completed.contains(oldCookie),
            "a delayed ACK of the old path must not complete the new route's quorum");
        // Changing the path for the same PathId exercises the parent routing contract.
        // Real replicas retain an immutable path/PathId association, so keep these
        // updates blocked and acknowledge them as their actual child recipients.
        auto acknowledge = [&](const auto& ev) {
            const auto& record = ev->Get()->GetRecord();
            auto ack = new TUpdateAck(record.GetOwner(), record.GetGeneration(), ev->Get()->GetPathId(),
                NSchemeBoard::GetPathVersion(record));
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, ack, 0, ev->Cookie),
                0, /* viaActorSystem */ true);
        };
        UNIT_ASSERT(freshAckIndex != updates.size());
        acknowledge(updates[freshAckIndex]);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_C(!completed.contains(oldCookie),
            "ACKs accumulated for the old path must not substitute for fresh replay ACKs");
        for (const auto& ev : updates) {
            if (newSelection.contains(childToService.at(ev->Recipient))) {
                acknowledge(ev);
            }
        }
        runtime.WaitFor("both publications completed", [&] { return completed.size() == 2; }, TDuration::Seconds(10));
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(completed.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(completed.at(oldCookie), 1);
        UNIT_ASSERT_VALUES_EQUAL(completed.at(oldCookie + 1), 2);
        runtime.Send(new IEventHandle(populator, monEdge, new TSchemeBoardMonEvents::TEvInfoRequest(100)));
        auto response = runtime.GrabEdgeEvent<TSchemeBoardMonEvents::TEvInfoResponse>(monEdge);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetPopulatorResponse().UpdateAcksSize(), 0);
    }

    Y_UNIT_TEST(ReplayUsesCurrentDescriptionPath) {
        TestReplayUsesCurrentDescriptionPath(false);
    }

    Y_UNIT_TEST(ReplaySurvivesPathAndReplicaReplacement) {
        TestReplayUsesCurrentDescriptionPath(true);
    }

    Y_UNIT_TEST(PathAndPathIdSelectionsRequireSeparateQuorums) {
        TTestBasicRuntime runtime;
        TStateStorageInfo::TRingGroup group{};
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

        auto select = [&](ui64 key) {
            TStateStorageInfo::TSelection selection;
            info->SelectReplicas(key, &selection, 0);
            return THashSet<TActorId>(selection.begin(), selection.end());
        };
        const auto idSelection = select(GetSchemeBoardSelectionKey(pathId));
        TString path;
        THashSet<TActorId> pathSelection;
        size_t pathOnly = 0;
        for (size_t i = 0; i != 1000; ++i) {
            path = TStringBuilder() << "/Root/TestPath" << i;
            pathSelection = select(GetSchemeBoardSelectionKey(path));
            pathOnly = 0;
            for (const auto& replica : pathSelection) {
                pathOnly += !idSelection.contains(replica);
            }
            if (pathOnly >= 2) {
                break;
            }
        }
        UNIT_ASSERT_C(pathOnly >= 2, "test path must select a sufficiently different replica set");

        constexpr ui64 cookie = 12345;
        TBlockEvents<TUpdateAck> ackBlocker(runtime, [&](const TUpdateAck::TPtr& ev) {
            return ev->Recipient == populator && ev->Cookie == cookie;
        });
        auto desc = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
        desc->Record = CreateSamplePathDescription(owner, pathId, path);
        runtime.Send(new IEventHandle(populator, edge, desc.Release(), 0, cookie));
        const size_t expectedAcks = idSelection.size() + pathOnly;
        runtime.WaitFor("ACKs from both selections", [&] { return ackBlocker.size() == expectedAcks; }, TDuration::Seconds(10));

        TVector<TUpdateAck::TPtr> pathOnlyAcks;
        TVector<TUpdateAck::TPtr> idAcks;
        for (const auto& ack : ackBlocker) {
            const auto& replica = childToService.at(ack->Sender);
            if (pathSelection.contains(replica) && !idSelection.contains(replica)) {
                pathOnlyAcks.emplace_back(ack);
            }
            if (idSelection.contains(replica)) {
                idAcks.emplace_back(ack);
            }
        }
        UNIT_ASSERT(pathOnlyAcks.size() >= 2);
        UNIT_ASSERT(idAcks.size() >= 2);
        runtime.Send(pathOnlyAcks[0].Release());
        runtime.Send(pathOnlyAcks[1].Release());
        UNIT_ASSERT_VALUES_EQUAL_C(CountEvents<TUpdateAck>(runtime, false, edge), 0,
            "a majority of the path selection must not substitute for the PathId selection quorum");

        runtime.Send(idAcks[0].Release());
        runtime.Send(idAcks[1].Release());
        auto mainAck = runtime.GrabEdgeEvent<TUpdateAck>(edge, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(mainAck->Get()->GetPathId(), pathId);
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
