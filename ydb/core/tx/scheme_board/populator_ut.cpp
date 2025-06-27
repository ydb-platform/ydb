#include "events_schemeshard.h"
#include "populator.h"
#include "replica.h"
#include "ut_helpers.h"

#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/wait_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/log.h>
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

    using TUpdateAck = NSchemeshardEvents::TEvUpdateAck;

    void AddReplicas(TStateStorageInfo::TRingGroup& group, const TVector<TActorId>& replicas) {
        group.NToSelect = replicas.size();
        group.Rings.resize(replicas.size());
        for (size_t i = 0; i < replicas.size(); ++i) {
            group.Rings[i].Replicas.push_back(replicas[i]);
        }
    }

    TIntrusivePtr<TStateStorageInfo> GenerateStateStorageInfo(const TVector<TStateStorageInfo::TRingGroup>& ringGroups) {
        auto info = MakeIntrusive<TStateStorageInfo>();
        info->RingGroups = ringGroups;
        return info;
    }

    using TStateStorageSetupper = std::function<void(TTestActorRuntime&, ui32)>;

    constexpr int ReplicasInRingGroup = 3;

    TStateStorageSetupper CreateCustomStateStorageSetupper(const TVector<TStateStorageInfo::TRingGroup>& ringGroups) {
        return [ringGroups](TTestActorRuntime& runtime, ui32 nodeIndex) {
            auto ssInfo = GenerateStateStorageInfo(ringGroups);
            auto sbInfo = GenerateStateStorageInfo(ringGroups);
            auto bInfo = GenerateStateStorageInfo(ringGroups);

            for (size_t ringGroup = 0; ringGroup < ringGroups.size(); ++ringGroup) {
                TVector<TActorId> ssreplicas;
                for (int i = 0; i < ReplicasInRingGroup; ++i) {
                    ssreplicas.emplace_back(MakeStateStorageReplicaID(runtime.GetNodeId(0), ReplicasInRingGroup * ringGroup + i));
                }
                AddReplicas(ssInfo->RingGroups[ringGroup], ssreplicas);

                TVector<TActorId> sbreplicas;
                for (int i = 0; i < ReplicasInRingGroup; ++i) {
                    sbreplicas.emplace_back(MakeSchemeBoardReplicaID(runtime.GetNodeId(0), ReplicasInRingGroup * ringGroup + i));
                }
                AddReplicas(sbInfo->RingGroups[ringGroup], sbreplicas);

                TVector<TActorId> breplicas;
                for (int i = 0; i < ReplicasInRingGroup; ++i) {
                    breplicas.emplace_back(MakeBoardReplicaID(runtime.GetNodeId(0), ReplicasInRingGroup * ringGroup + i));
                }
                AddReplicas(bInfo->RingGroups[ringGroup], breplicas);

                if (nodeIndex == 0) {
                    for (ui32 i = 0; i < ReplicasInRingGroup; ++i) {
                        runtime.AddLocalService(ssreplicas[i],
                            TActorSetupCmd(CreateStateStorageReplica(ssInfo.Get(), ReplicasInRingGroup * ringGroup + i), TMailboxType::Revolving, 0),
                            nodeIndex
                        );
                        runtime.AddLocalService(sbreplicas[i],
                            TActorSetupCmd(CreateSchemeBoardReplica(sbInfo.Get(), ReplicasInRingGroup * ringGroup + i), TMailboxType::Revolving, 0),
                            nodeIndex
                        );
                        runtime.AddLocalService(breplicas[i],
                            TActorSetupCmd(CreateStateStorageBoardReplica(bInfo.Get(), ReplicasInRingGroup * ringGroup + i), TMailboxType::Revolving, 0),
                            nodeIndex
                        );
                    }
                }
            }

            const TActorId ssproxy = MakeStateStorageProxyID();
            runtime.AddLocalService(ssproxy,
                TActorSetupCmd(CreateStateStorageProxy(ssInfo.Get(), bInfo.Get(), sbInfo.Get()), TMailboxType::Revolving, 0),
                nodeIndex
            );
        };
    }

    TStateStorageSetupper CreateDefaultStateStorageSetupper() {
        return CreateCustomStateStorageSetupper({ TStateStorageInfo::TRingGroup{} });
    }

    void AddDomain(
        TTestActorRuntime& runtime,
        TAppPrepare& app,
        const TString& name,
        ui32 domainUid,
        ui64 hiveTabletId,
        ui64 schemeshardTabletId
    ) {
        app.ClearDomainsAndHive();
        ui32 planResolution = 50;
        auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
            name, domainUid, schemeshardTabletId,
            planResolution,
            TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(1)},
            TVector<ui64>{},
            TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(1)},
            DefaultPoolKinds()
        );

        TVector<ui64> ids = runtime.GetTxAllocatorTabletIds();
        ids.insert(ids.end(), domain->TxAllocators.begin(), domain->TxAllocators.end());
        runtime.SetTxAllocatorTabletIds(ids);

        app.AddDomain(domain.Release());
        app.AddHive(hiveTabletId);
    }

    void SetupRuntime(TTestActorRuntime& runtime, const TStateStorageSetupper& setupStateStorage = CreateDefaultStateStorageSetupper()) {
        for (ui32 i : xrange(runtime.GetNodeCount())) {
            setupStateStorage(runtime, i);
        }

        TAppPrepare app;
        AddDomain(runtime, app, "Root", 0, TTestTxConfig::Hive, TTestTxConfig::SchemeShard);
        SetupChannelProfiles(app, 1);
        runtime.Initialize(app.Unwrap());
    }

    TIntrusiveConstPtr<TStateStorageInfo> GetStateStorageConfig(TTestActorRuntime& runtime) {
        const TActorId recipient = MakeStateStorageProxyID();
        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.Send(recipient, edge, new TEvStateStorage::TEvListSchemeBoard(false));

        auto result = runtime.GrabEdgeEvent<TEvStateStorage::TEvListSchemeBoardResult>(edge);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Get()->Info);

        return result->Get()->Info;
    }

    NKikimrScheme::TEvDescribeSchemeResult CreateSamplePathDescription(ui64 owner, const TPathId& pathId, const TString& path) {
        NKikimrScheme::TEvDescribeSchemeResult desc;

        desc.SetStatus(NKikimrScheme::StatusSuccess);
        desc.SetPathOwnerId(owner);
        desc.SetPathId(pathId.LocalPathId);
        desc.SetPath(path);

        auto& self = *desc.MutablePathDescription()->MutableSelf();
        self.SetName("TestPath");
        self.SetPathId(pathId.LocalPathId);
        self.SetSchemeshardId(owner);
        self.SetPathType(NKikimrSchemeOp::EPathTypeDir);
        self.SetCreateFinished(true);
        self.SetCreateTxId(1);
        self.SetCreateStep(1);
        self.SetParentPathId(NSchemeShard::RootPathId);
        self.SetPathState(NKikimrSchemeOp::EPathStateNoChanges);
        self.SetPathVersion(1);

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
            if (ShouldIgnore(ringGroups[ringGroup]) || IsMajorityReached(ringGroups[ringGroup], ringGroupAcks[ringGroup])) {
                // not required for quorum
                continue;
            }
            ringGroupAcks[ringGroup]++;
            requiredAcks.emplace_back(event);
        }

        return requiredAcks;
    }

    void TestPopulatorQuorum(TVector<TStateStorageInfo::TRingGroup>&& ringGroupsConfiguration) {
        TTestBasicRuntime runtime;
        SetupRuntime(runtime, CreateCustomStateStorageSetupper(ringGroupsConfiguration));

        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NLog::PRI_DEBUG);

        const TActorId edge = runtime.AllocateEdgeActor();
        constexpr ui64 owner = TTestTxConfig::SchemeShard;

        auto groupInfo = GetStateStorageConfig(runtime);
        const auto replicas = groupInfo->SelectAllReplicas();
        UNIT_ASSERT_VALUES_EQUAL_C(
            replicas.size(),
            ReplicasInRingGroup * ringGroupsConfiguration.size(),
            groupInfo->ToString()
        );
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

        ackBlocker.Stop();
        auto requiredAcks = GetAcksRequiredForQuorum(ackBlocker, populatorToReplicaMap, groupInfo->RingGroups);
        UNIT_ASSERT(!requiredAcks.empty());
        // resend all required acks except the last one
        for (int i = 0; i + 1 < ssize(requiredAcks); ++i) {
            runtime.Send(requiredAcks[i].Release());
        }
        UNIT_CHECK_GENERATED_EXCEPTION(
            runtime.GrabEdgeEvent<TUpdateAck>(edge, TDuration::Seconds(10)),
            TEmptyEventQueueException
        );
        runtime.Send(requiredAcks.back().Release());

        auto mainAck = runtime.GrabEdgeEvent<TUpdateAck>(edge, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(mainAck->Sender, populator, mainAck->ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(mainAck->Get()->GetPathId(), pathId, mainAck->ToString());
    }

    Y_UNIT_TEST(OneRingGroup) {
        TestPopulatorQuorum({ {} });
    }

    Y_UNIT_TEST(TwoRingGroups) {
        TestPopulatorQuorum({ {.State = PRIMARY}, {.State = SYNCHRONIZED} });
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
