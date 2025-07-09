#include "ut_helpers.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>

#include <ydb/library/actors/core/log.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NSchemeBoard {

class TSubscriberTest: public NUnitTest::TTestBase {
    TVector<TActorId> ResolveReplicas() {
        auto allReplicas = GetStateStorageInfo(*Context)->SelectAllReplicas();
        return TVector<TActorId>(allReplicas.begin(), allReplicas.end());
    }

public:
    void SetUp() override {
        Context = MakeHolder<TTestContext>(2);

        for (ui32 i : xrange(Context->GetNodeCount())) {
            SetupStateStorage(*Context, i, 0);
        }

        Context->Initialize(TAppPrepare().Unwrap());
        Context->SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NLog::PRI_DEBUG);
    }

    void TearDown() override {
        Context.Reset();
    }

    UNIT_TEST_SUITE(TSubscriberTest);
    UNIT_TEST(Boot);
    UNIT_TEST(NotifyUpdate);
    UNIT_TEST(NotifyDelete);
    UNIT_TEST(StrongNotificationAfterCommit);
    UNIT_TEST(InvalidNotification);
    UNIT_TEST(ReconnectOnFailure);
    UNIT_TEST(Sync);
    UNIT_TEST(SyncPartial);
    UNIT_TEST(SyncWithOutdatedReplica);
    UNIT_TEST_SUITE_END();

    void Boot();
    void NotifyUpdate();
    void NotifyDelete();
    void StrongNotificationAfterCommit();
    void InvalidNotification();
    void ReconnectOnFailure();
    void Sync();
    void SyncPartial();
    void SyncWithOutdatedReplica();

private:
    THolder<TTestContext> Context;

}; // TSubscriberTest

UNIT_TEST_SUITE_REGISTRATION(TSubscriberTest);

void TSubscriberTest::Boot() {
    const TActorId edge = Context->AllocateEdgeActor();

    Context->CreateSubscriber(edge, "path");
    auto ev = Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyDelete>(edge);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->Path);
}

void TSubscriberTest::NotifyUpdate() {
    const TActorId edge = Context->AllocateEdgeActor();

    Context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyDelete>(edge, "path");

    auto replicas = ResolveReplicas();

    Context->HandshakeReplica(replicas[0], edge);
    Context->Send(replicas[0], edge, GenerateUpdate(GenerateDescribe("path", TPathId(1, 1))));
    auto ev = Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->Path);
}

void TSubscriberTest::NotifyDelete() {
    const TActorId edge = Context->AllocateEdgeActor();

    auto replicas = ResolveReplicas();
    Y_ABORT_UNLESS(replicas.size() > 2);

    for (const auto& replica : replicas) {
        Context->HandshakeReplica(replica, edge);
        Context->Send(replica, edge, GenerateUpdate(GenerateDescribe("path", TPathId(1, 1))));
    }

    Context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyUpdate>(edge, "path");

    for (size_t i = 0; i < replicas.size() / 2 + 1; ++i) {
        Context->Send(replicas[i], edge, GenerateUpdate(GenerateDescribe("path", TPathId(1, 1)), 1, 1, true));
    }

    auto ev = Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyDelete>(edge);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->Path);
}

void TSubscriberTest::StrongNotificationAfterCommit() {
    const TActorId edge = Context->AllocateEdgeActor();

    Context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyDelete>(edge, "path", 1, false);
    {
        auto ev = Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyDelete>(edge);
        UNIT_ASSERT(ev->Get());
        UNIT_ASSERT(!ev->Get()->Strong);
    }

    for (const auto& replica : ResolveReplicas()) {
        Context->HandshakeReplica(replica, edge);
        Context->CommitReplica(replica, edge);
    }

    {
        auto ev = Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyDelete>(edge);
        UNIT_ASSERT(ev->Get());
        UNIT_ASSERT(ev->Get()->Strong);
    }
}

void TSubscriberTest::InvalidNotification() {
    const TActorId edge = Context->AllocateEdgeActor();

    const TActorId subscriber = Context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyDelete>(edge, "path");

    // send notification directly to subscriber
    auto* notify = new NInternalEvents::TEvNotifyBuilder(TPathId(1, 1));
    notify->SetPathDescription(MakeOpaquePathDescription("", GenerateDescribe("another/path", TPathId(1, 1))));
    Context->Send(subscriber, edge, notify);

    size_t counter = Context->CountEdgeEvents<TSchemeBoardEvents::TEvNotifyUpdate>();
    UNIT_ASSERT_VALUES_EQUAL(0, counter);
}

void TSubscriberTest::ReconnectOnFailure() {
    const TActorId edge = Context->AllocateEdgeActor(1);

    Context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyDelete>(edge, "path", 1, true, 1);

    Context->Disconnect(0, 1);
    Context->Connect(0, 1);

    auto replicas = ResolveReplicas();

    Context->HandshakeReplica(replicas[0], edge);
    Context->Send(replicas[0], edge, GenerateUpdate(GenerateDescribe("path", TPathId(1, 1))));
    auto ev = Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->Path);
}

void TSubscriberTest::Sync() {
    const TActorId edge = Context->AllocateEdgeActor();

    auto replicas = ResolveReplicas();
    for (const auto& replica : replicas) {
        Context->HandshakeReplica(replica, edge);
        Context->Send(replica, edge, GenerateUpdate(GenerateDescribe("path", TPathId(1, 1))));
    }

    const TActorId subscriber = Context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyUpdate>(edge, "path");
    Context->Send(subscriber, edge, new NInternalEvents::TEvSyncRequest(), 0, 1);
    auto ev = Context->GrabEdgeEvent<NInternalEvents::TEvSyncResponse>(edge);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->Path);
    UNIT_ASSERT_VALUES_EQUAL(false, ev->Get()->Partial);
}

void TSubscriberTest::SyncPartial() {
    const TActorId edge = Context->AllocateEdgeActor();
    const TActorId subscriber = Context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyDelete>(edge, "path");

    ui64 syncCookie = 0;
    auto replicas = ResolveReplicas();
    for (ui32 i : xrange(replicas.size())) {
        Context->Send(replicas[i], edge, new TEvents::TEvPoisonPill());

        Context->Send(subscriber, edge, new NInternalEvents::TEvSyncRequest(), 0, ++syncCookie);
        auto ev = Context->GrabEdgeEvent<NInternalEvents::TEvSyncResponse>(edge);

        UNIT_ASSERT(ev->Get());
        UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->Path);
        UNIT_ASSERT_VALUES_EQUAL((i + 1) > (replicas.size() / 2), ev->Get()->Partial);
    }
}

void TSubscriberTest::SyncWithOutdatedReplica() {
    const TActorId edge = Context->AllocateEdgeActor();

    auto replicas = ResolveReplicas();
    UNIT_ASSERT(replicas.size() > 2);

    for (ui32 i = 0; i < replicas.size(); ++i) {
        const auto& replica = replicas.at(i);
        Context->HandshakeReplica(replica, edge);

        if (i == 0) {
            // outdated, but greater version
            Context->Send(replica, edge, GenerateUpdate(GenerateDescribe("path", TPathId(1, 2), 2, TDomainId(1, 1))));
        } else {
            // up to date
            Context->Send(replica, edge, GenerateUpdate(GenerateDescribe("path", TPathId(2, 2), 1, TDomainId(2, 1))));
        }
    }

    const TActorId subscriber = Context->CreateSubscriber(edge, "path");
    {
        auto ev = Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);
        UNIT_ASSERT(ev->Get());
        UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->Path);
        UNIT_ASSERT_VALUES_EQUAL(TPathId(2, 2), ev->Get()->PathId);
    }

    Context->Send(subscriber, edge, new NInternalEvents::TEvSyncRequest(), 0, 1);
    {
        auto ev = Context->GrabEdgeEvent<NInternalEvents::TEvSyncResponse>(edge);
        UNIT_ASSERT(ev->Get());
        UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->Path);
        UNIT_ASSERT(!ev->Get()->Partial);
    }
}

class TSubscriberCombinationsTest: public NUnitTest::TTestBase {
    TVector<TActorId> ResolveReplicas(TTestContext& context) {
        const TActorId proxy = MakeStateStorageProxyID();
        const TActorId edge = context.AllocateEdgeActor();

        context.Send(proxy, edge, new TEvStateStorage::TEvListSchemeBoard(false));
        auto ev = context.GrabEdgeEvent<TEvStateStorage::TEvListSchemeBoardResult>(edge);

        Y_ABORT_UNLESS(ev->Get()->Info);
        auto allReplicas = ev->Get()->Info->SelectAllReplicas();
        return TVector<TActorId>(allReplicas.begin(), allReplicas.end());
    }

    THolder<TTestContext> CreateContext() {
        auto context = MakeHolder<TTestContext>(2);

        for (ui32 i : xrange(context->GetNodeCount())) {
            SetupStateStorage(*context, i, 0);
        }

        context->Initialize(TAppPrepare().Unwrap());
        context->SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NLog::PRI_DEBUG);
        context->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NLog::PRI_DEBUG);

        return context;
    }

    UNIT_TEST_SUITE(TSubscriberCombinationsTest);
    UNIT_TEST(CombinationsRootDomain);
    UNIT_TEST(MigratedPathRecreation);
    UNIT_TEST(CombinationsMigratedPath);
    UNIT_TEST_SUITE_END();

    void CombinationsRootDomain();
    void MigratedPathRecreation();
    void CombinationsMigratedPath();
}; // TSubscriberCombinationsTest

UNIT_TEST_SUITE_REGISTRATION(TSubscriberCombinationsTest);


void TSubscriberCombinationsTest::CombinationsRootDomain() {
    TString path = "/root/tenant";
    ui64 gssOwnerID = 800;
    TVector<TCombinationsArgs> combinations = GenerateCombinationsDomainRoot(path, gssOwnerID);

    //make all the variants
    for (const auto& argsLeft: combinations) {
        for (const auto& argsRight: combinations) {
            Cerr << "=========== " << argsLeft.GenerateDescribe().ShortDebugString()
                 << "\n=========== " << argsRight.GenerateDescribe().ShortDebugString() << Endl;

            auto context = CreateContext();

            TVector<TActorId> replicas = ResolveReplicas(*context);
            Y_ASSERT(replicas.size() >= 2);

            const TActorId populatorLeft = context->AllocateEdgeActor();
            context->HandshakeReplica(replicas[0], populatorLeft, argsLeft.OwnerId, argsLeft.Generation);
            context->CommitReplica(replicas[0], populatorLeft, argsLeft.OwnerId, argsLeft.Generation);

            const TActorId populatorRight = context->AllocateEdgeActor();
            context->HandshakeReplica(replicas[1], populatorRight, argsRight.OwnerId, argsRight.Generation);
            context->CommitReplica(replicas[1], populatorRight, argsRight.OwnerId, argsRight.Generation);

            const TActorId edge = context->AllocateEdgeActor();
            context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyDelete>(edge, path);

            context->Send(replicas[0], populatorLeft, argsLeft.GenerateUpdate());

            if (!argsLeft.IsDeletion) {
                Cerr << "=========== !argsLeft.IsDeletion" << Endl;
                auto ev = context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);
                Y_ASSERT(ev);
                UNIT_ASSERT_VALUES_EQUAL(path, ev->Get()->Path);
                UNIT_ASSERT_VALUES_EQUAL(argsLeft.PathId, ev->Get()->PathId);
                const NKikimrScheme::TEvDescribeSchemeResult& descr = ev->Get()->DescribeSchemeResult;
                const auto& domainKey = descr.GetPathDescription().GetDomainDescription().GetDomainKey();
                UNIT_ASSERT_VALUES_EQUAL(argsLeft.DomainId, TDomainId(domainKey.GetSchemeShard(), domainKey.GetPathId()));
            }

            context->Send(replicas[1], populatorRight, argsRight.GenerateUpdate());

            if (argsLeft.GetSuperId() >= argsRight.GetSuperId()) {
                Cerr << "=========== argsLeft.GetSuperId() >= argsRight.GetSuperId()" << Endl;

                // there is no update comming
                continue;
            }

            Cerr << "=========== argsLeft.GetSuperId() < argsRight.GetSuperId()" << Endl;

            if (!argsRight.IsDeletion) {
                Cerr << "=========== !argsRight.IsDeletion" << Endl;

                auto ev = context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);

                Y_ASSERT(ev);
                UNIT_ASSERT_VALUES_EQUAL(path, ev->Get()->Path);
                UNIT_ASSERT_VALUES_EQUAL(argsRight.PathId, ev->Get()->PathId);
                const NKikimrScheme::TEvDescribeSchemeResult& descr = ev->Get()->DescribeSchemeResult;
                const auto& domainKey = descr.GetPathDescription().GetDomainDescription().GetDomainKey();
                UNIT_ASSERT_VALUES_EQUAL(argsRight.DomainId, TDomainId(domainKey.GetSchemeShard(), domainKey.GetPathId()));

                continue;
            }
        }
    }
}

void TSubscriberCombinationsTest::MigratedPathRecreation() {
    ui64 gssOwnerID = 800;
    ui64 tssOwnerID = 900;

    TString path = "/root/db/dir_inside";

    auto domainId = TPathId(gssOwnerID, 1);
    auto migratedPathId = TPathId(gssOwnerID, 1111);

    auto recreatedPathId = TPathId(tssOwnerID, 11);

    auto argsLeft = TCombinationsArgs{
        path, migratedPathId, 1, domainId,
        gssOwnerID, 1, false};
    auto argsRight = TCombinationsArgs{
        path, recreatedPathId, 1, domainId,
        tssOwnerID, 1, false};


    auto context = CreateContext();

    TVector<TActorId> replicas = ResolveReplicas(*context);
    Y_ASSERT(replicas.size() >= 2);

    const TActorId populatorLeft = context->AllocateEdgeActor();
    context->HandshakeReplica(replicas[0], populatorLeft, argsLeft.OwnerId, argsLeft.Generation);
    context->CommitReplica(replicas[0], populatorLeft, argsLeft.OwnerId, argsLeft.Generation);

    const TActorId populatorRight = context->AllocateEdgeActor();
    context->HandshakeReplica(replicas[1], populatorRight, argsRight.OwnerId, argsRight.Generation);
    context->CommitReplica(replicas[1], populatorRight, argsRight.OwnerId, argsRight.Generation);

    const TActorId edge = context->AllocateEdgeActor();
    context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyDelete>(edge, path);

    context->Send(replicas[0], populatorLeft, argsLeft.GenerateUpdate());

    if (!argsLeft.IsDeletion) {
        Cerr << "=========== !argsLeft.IsDeletion" << Endl;
        auto ev = context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);
        Y_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(path, ev->Get()->Path);
        UNIT_ASSERT_VALUES_EQUAL(argsLeft.PathId, ev->Get()->PathId);
        const NKikimrScheme::TEvDescribeSchemeResult& descr = ev->Get()->DescribeSchemeResult;
        const auto& domainKey = descr.GetPathDescription().GetDomainDescription().GetDomainKey();
        UNIT_ASSERT_VALUES_EQUAL(argsLeft.DomainId, TDomainId(domainKey.GetSchemeShard(), domainKey.GetPathId()));
    }

    context->Send(replicas[1], populatorRight, argsRight.GenerateUpdate());

    if (argsLeft.GetSuperId() >= argsRight.GetSuperId()) {
        Cerr << "=========== argsLeft.GetSuperId() >= argsRight.GetSuperId()" << Endl;

        // there is no update comming
        return;
    }

    Cerr << "=========== argsLeft.GetSuperId() < argsRight.GetSuperId()" << Endl;

    if (!argsRight.IsDeletion) {
        Cerr << "=========== !argsRight.IsDeletion" << Endl;

        auto ev = context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);

        Y_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(path, ev->Get()->Path);
        UNIT_ASSERT_VALUES_EQUAL(argsRight.PathId, ev->Get()->PathId);
        const NKikimrScheme::TEvDescribeSchemeResult& descr = ev->Get()->DescribeSchemeResult;
        const auto& domainKey = descr.GetPathDescription().GetDomainDescription().GetDomainKey();
        UNIT_ASSERT_VALUES_EQUAL(argsRight.DomainId, TDomainId(domainKey.GetSchemeShard(), domainKey.GetPathId()));

        return;
    }
}

void TSubscriberCombinationsTest::CombinationsMigratedPath() {
    TString path = "/Root/Tenant/table_inside";
    ui64 gssID = 800;
    ui64 tssID = 900;
    ui64 tssIDrecreated = 910;
    ui64 gssLocalId = 5;
    ui64 tssLocalId = 9;
    TVector<TCombinationsArgs> combinations = GenerateCombinationsMigratedPath(path, gssID, {tssID, tssIDrecreated}, gssLocalId, tssLocalId);

    //make all the variants
    for (const auto& argsLeft: combinations) {
        for (const auto& argsRight: combinations) {
            Cerr << "=========== " << argsLeft.GenerateDescribe().ShortDebugString()
                 << "\n=========== " << argsRight.GenerateDescribe().ShortDebugString() << Endl;

            auto context = CreateContext();

            TVector<TActorId> replicas = ResolveReplicas(*context);
            Y_ASSERT(replicas.size() >= 2);

            const TActorId populatorLeft = context->AllocateEdgeActor();
            context->HandshakeReplica(replicas[0], populatorLeft, argsLeft.OwnerId, argsLeft.Generation);
            context->CommitReplica(replicas[0], populatorLeft, argsLeft.OwnerId, argsLeft.Generation);

            const TActorId populatorRight = context->AllocateEdgeActor();
            context->HandshakeReplica(replicas[1], populatorRight, argsRight.OwnerId, argsRight.Generation);
            context->CommitReplica(replicas[1], populatorRight, argsRight.OwnerId, argsRight.Generation);

            const TActorId edge = context->AllocateEdgeActor();
            context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyDelete>(edge, path);

            context->Send(replicas[0], populatorLeft, argsLeft.GenerateUpdate());

            if (!argsLeft.IsDeletion) {
                Cerr << "=========== !argsLeft.IsDeletion" << Endl;
                auto ev = context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);
                Y_ASSERT(ev);
                UNIT_ASSERT_VALUES_EQUAL(path, ev->Get()->Path);
                UNIT_ASSERT_VALUES_EQUAL(argsLeft.PathId, ev->Get()->PathId);
                const NKikimrScheme::TEvDescribeSchemeResult& descr = ev->Get()->DescribeSchemeResult;
                const auto& domainKey = descr.GetPathDescription().GetDomainDescription().GetDomainKey();
                UNIT_ASSERT_VALUES_EQUAL(argsLeft.DomainId, TDomainId(domainKey.GetSchemeShard(), domainKey.GetPathId()));
            }

            context->Send(replicas[1], populatorRight, argsRight.GenerateUpdate());

            if (argsLeft.GetSuperId() >= argsRight.GetSuperId()) {
                Cerr << "=========== argsLeft.GetSuperId() >= argsRight.GetSuperId()" << Endl;

                // there is no update comming
                continue;
            }

            Cerr << "=========== argsLeft.GetSuperId() < argsRight.GetSuperId()" << Endl;

            if (!argsRight.IsDeletion) {
                Cerr << "=========== !argsRight.IsDeletion" << Endl;

                auto ev = context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);

                Y_ASSERT(ev);
                UNIT_ASSERT_VALUES_EQUAL(path, ev->Get()->Path);
                UNIT_ASSERT_VALUES_EQUAL(argsRight.PathId, ev->Get()->PathId);
                const NKikimrScheme::TEvDescribeSchemeResult& descr = ev->Get()->DescribeSchemeResult;
                const auto& domainKey = descr.GetPathDescription().GetDomainDescription().GetDomainKey();
                UNIT_ASSERT_VALUES_EQUAL(argsRight.DomainId, TDomainId(domainKey.GetSchemeShard(), domainKey.GetPathId()));

                continue;
            }
        }
    }
}

namespace {

using TEvNotifyUpdate = TSchemeBoardEvents::TEvNotifyUpdate;
using TEvNotifyDelete = TSchemeBoardEvents::TEvNotifyDelete;

constexpr int ReplicasInRingGroup = 3;

NInternalEvents::TEvHandshakeResponse::TPtr HandshakeReplica(
    TTestActorRuntime& runtime,
    const TActorId& replica,
    const TActorId& sender,
    ui64 owner = 1,
    ui64 generation = 1,
    bool grabResponse = true
) {
    runtime.Send(replica, sender, new NInternalEvents::TEvHandshakeRequest(owner, generation));

    if (grabResponse) {
        return runtime.GrabEdgeEvent<NInternalEvents::TEvHandshakeResponse>(sender);
    }

    return nullptr;
}

bool ShouldIgnore(const TStateStorageInfo::TRingGroup& ringGroup) {
    return ringGroup.WriteOnly || ringGroup.State == ERingGroupState::DISCONNECTED;
}

ui32 CountReplicas(const TStateStorageInfo::TRingGroup& ringGroup) {
    ui32 replicas = 0;
    for (const auto& ring : ringGroup.Rings) {
        replicas += ring.Replicas.size();
    }
    return replicas;
}

ui32 CountParticipatingReplicas(const TStateStorageInfo& info) {
    ui32 participatingReplicas = 0;
    for (const auto& ringGroup : info.RingGroups) {
        if (ShouldIgnore(ringGroup)) {
            continue;
        }
        participatingReplicas += CountReplicas(ringGroup);
    }
    return participatingReplicas;
}

TVector<TActorId> GetReplicasRequiredForQuorum(const TVector<TStateStorageInfo::TRingGroup>& ringGroups) {
    TVector<TActorId> requiredReplicas;
    TVector<ui32> updatesByGroup(ringGroups.size(), 0);

    for (size_t i = 0; i < ringGroups.size(); ++i) {
        const auto& ringGroup = ringGroups[i];
        if (ShouldIgnore(ringGroup)) {
            // not participating in the quorum
            continue;
        }
        const auto majority = CountReplicas(ringGroup) / 2 + 1;
        for (const auto& ring : ringGroup.Rings) {
            for (const auto& replica : ring.Replicas) {
                if (updatesByGroup[i] < majority) {
                    requiredReplicas.emplace_back(replica);
                    ++updatesByGroup[i];
                }
            }
        }
    }

    return requiredReplicas;
}

TStateStorageInfo::TRingGroup GetReplicaRingGroup(TActorId target, const TVector<TStateStorageInfo::TRingGroup>& ringGroups) {
    for (const auto& ringGroup : ringGroups) {
        for (const auto& ring : ringGroup.Rings) {
            for (const auto& replica : ring.Replicas) {
                if (replica == target) {
                    return ringGroup;
                }
            }
        }
    }
    UNIT_FAIL("Replica: " << target << " is not a part of any ring group.");
    return {};
}

TActorId GetFirstParticipatingReplica(const TVector<TStateStorageInfo::TRingGroup>& ringGroups) {
    for (const auto& ringGroup : ringGroups) {
        if (ShouldIgnore(ringGroup)) {
            continue;
        }
        for (const auto& ring : ringGroup.Rings) {
            for (const auto& replica : ring.Replicas) {
                return replica;
            }
        }
    }
    UNIT_FAIL("All the replicas are ignored.");
    return {};
}

}

Y_UNIT_TEST_SUITE(TSubscriberSinglePathUpdateTest) {

    void TestSinglePathUpdate(TVector<TStateStorageInfo::TRingGroup>&& ringGroupsConfiguration) {
        TTestBasicRuntime runtime;
        SetupMinimalRuntime(runtime, CreateCustomStateStorageSetupper(ringGroupsConfiguration, ReplicasInRingGroup));

        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NLog::PRI_DEBUG);

        const auto stateStorageInfo = GetStateStorageInfo(runtime);
        const auto participatingReplicas = CountParticipatingReplicas(*stateStorageInfo);

        constexpr int DomainId = 1;
        constexpr TPathId PathId = TPathId(DomainId, 1);
        constexpr const char* Path = "TestPath";
        const TActorId edge = runtime.AllocateEdgeActor();

        const TActorId subscriber = runtime.Register(CreateSchemeBoardSubscriber(edge, Path, DomainId));
        TBlockEvents<NInternalEvents::TEvNotify> notificationBlocker(runtime, [&](const NInternalEvents::TEvNotify::TPtr& ev) {
            return ev->Recipient == subscriber;
        });
        runtime.WaitFor("initial path lookups", [&]() {
            return notificationBlocker.size() == participatingReplicas;
        }, TDuration::Seconds(10));
        notificationBlocker.Stop().Unblock();

        int pathVersion = 0;
        for (const auto& ringGroup : stateStorageInfo->RingGroups) {
            const bool shouldIgnore = ShouldIgnore(ringGroup);
            for (const auto& ring : ringGroup.Rings) {
                for (const auto& replica : ring.Replicas) {
                    HandshakeReplica(runtime, replica, edge);
                    Cerr << "Sending path update to replica: " << replica << '\n';
                    runtime.Send(replica, edge, GenerateUpdate(GenerateDescribe(Path, PathId, ++pathVersion)));
                    if (shouldIgnore) {
                        // Every such check takes at least a minute!
                        // Make sure that there are not many ignored replicas.
                        UNIT_CHECK_GENERATED_EXCEPTION(
                            runtime.GrabEdgeEvent<TEvNotifyUpdate>(edge, TDuration::Seconds(10)),
                            TEmptyEventQueueException
                        );
                    } else {
                        const auto ev = runtime.GrabEdgeEvent<TEvNotifyUpdate>(edge, TDuration::Seconds(10));
                        UNIT_ASSERT_VALUES_EQUAL_C(ev->Sender, subscriber, ev->ToString());
                        UNIT_ASSERT_VALUES_EQUAL(Path, ev->Get()->Path);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(OneRingGroup) {
        TestSinglePathUpdate({ {} });
    }

    Y_UNIT_TEST(TwoRingGroups) {
        TestSinglePathUpdate({ {.State = PRIMARY}, {.State = SYNCHRONIZED} });
    }

    Y_UNIT_TEST(OneDisconnectedRingGroup) {
        TestSinglePathUpdate({ {.State = PRIMARY}, {.State = DISCONNECTED} });
    }

    Y_UNIT_TEST(OneSynchronizedRingGroup) {
        TestSinglePathUpdate({ {.State = PRIMARY}, {.State = SYNCHRONIZED} });
    }

    Y_UNIT_TEST(OneWriteOnlyRingGroup) {
        TestSinglePathUpdate({ {.State = PRIMARY}, {.State = PRIMARY, .WriteOnly = true} });
    }

    Y_UNIT_TEST(ReplicaConfigMismatch) {
        TTestBasicRuntime runtime;
        SetupMinimalRuntime(runtime);

        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NLog::PRI_DEBUG);

        const auto stateStorageInfo = GetStateStorageInfo(runtime);
        const auto participatingReplicas = CountParticipatingReplicas(*stateStorageInfo);

        constexpr int DomainId = 1;
        constexpr TPathId PathId = TPathId(DomainId, 1);
        constexpr const char* Path = "TestPath";
        const TActorId edge = runtime.AllocateEdgeActor();

        const TActorId subscriber = runtime.Register(CreateSchemeBoardSubscriber(edge, Path, DomainId));
        TBlockEvents<NInternalEvents::TEvNotify> notificationBlocker(runtime, [&](const NInternalEvents::TEvNotify::TPtr& ev) {
            return ev->Recipient == subscriber;
        });
        runtime.WaitFor("initial path lookups", [&]() {
            return notificationBlocker.size() == participatingReplicas;
        }, TDuration::Seconds(10));
        notificationBlocker.Stop().Unblock();

        const TActorId replica = GetFirstParticipatingReplica(stateStorageInfo->RingGroups);
        HandshakeReplica(runtime, replica, edge);
        ui64 pathVersion = 1;
        runtime.Send(replica, edge, GenerateUpdate(GenerateDescribe(Path, PathId, pathVersion)));
        {
            const auto ev = runtime.GrabEdgeEvent<TEvNotifyUpdate>(edge, TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Sender, subscriber, ev->ToString());
            UNIT_ASSERT_VALUES_EQUAL(Path, ev->Get()->Path);
        }

        auto newConfig = MakeIntrusive<TStateStorageInfo>(*stateStorageInfo);
        newConfig->ClusterStateGeneration++;
        runtime.Send(replica, edge, new TEvStateStorage::TEvUpdateGroupConfig(nullptr, nullptr, newConfig));

        ++pathVersion;
        runtime.Send(replica, edge, GenerateUpdate(GenerateDescribe(Path, PathId, pathVersion)));
        UNIT_CHECK_GENERATED_EXCEPTION(
            runtime.GrabEdgeEvent<TEvNotifyUpdate>(edge, TDuration::Seconds(10)),
            TEmptyEventQueueException
        );
    }
}

Y_UNIT_TEST_SUITE(TSubscriberSyncQuorumTest) {

    void TestSyncQuorum(TVector<TStateStorageInfo::TRingGroup>&& ringGroupsConfiguration) {
        TTestBasicRuntime runtime;
        SetupMinimalRuntime(runtime, CreateCustomStateStorageSetupper(ringGroupsConfiguration, ReplicasInRingGroup));

        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NLog::PRI_DEBUG);

        const auto stateStorageInfo = GetStateStorageInfo(runtime);
        const auto participatingReplicas = CountParticipatingReplicas(*stateStorageInfo);

        constexpr int DomainId = 1;
        constexpr const char* Path = "TestPath";
        const TActorId edge = runtime.AllocateEdgeActor();

        const TActorId subscriber = runtime.Register(CreateSchemeBoardSubscriber(edge, Path, DomainId));
        TBlockEvents<NInternalEvents::TEvNotify> notificationBlocker(runtime, [&](const NInternalEvents::TEvNotify::TPtr& ev) {
            return ev->Recipient == subscriber;
        });
        runtime.WaitFor("initial path lookups", [&]() {
            return notificationBlocker.size() == participatingReplicas;
        }, TDuration::Seconds(10));
        notificationBlocker.Stop().Unblock();

        const auto requiredReplicas = GetReplicasRequiredForQuorum(stateStorageInfo->RingGroups);
        for (const auto& replica : stateStorageInfo->SelectAllReplicas()) {
            if (!FindPtr(requiredReplicas, replica)) {
                Cerr << "Poisoning replica: " << replica << '\n';
                runtime.Send(replica, edge, new TEvents::TEvPoisonPill());
            }
        }

        ui64 cookie = 12345;
        {
            runtime.Send(new IEventHandle(subscriber, edge, new NInternalEvents::TEvSyncRequest(), 0, cookie));
            const auto syncResponse = runtime.GrabEdgeEvent<NInternalEvents::TEvSyncResponse>(edge);

            UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Path, Path, syncResponse->ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Partial, false, syncResponse->ToString());
        }

        {
            const auto replicaToKill = requiredReplicas[RandomNumber(requiredReplicas.size())];
            const auto& ringGroup = GetReplicaRingGroup(replicaToKill, stateStorageInfo->RingGroups);
            Cerr << "Poisoning replica: " << replicaToKill << " whose ring group state is: " << static_cast<int>(ringGroup.State) << '\n';
            runtime.Send(replicaToKill, edge, new TEvents::TEvPoisonPill());

            ++cookie;
            runtime.Send(new IEventHandle(subscriber, edge, new NInternalEvents::TEvSyncRequest(), 0, cookie));
            const auto syncResponse = runtime.GrabEdgeEvent<NInternalEvents::TEvSyncResponse>(edge);

            UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Path, Path, syncResponse->ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Partial, true, syncResponse->ToString());
        }
    }

    Y_UNIT_TEST(OneRingGroup) {
        TestSyncQuorum({ {} });
    }

    Y_UNIT_TEST(TwoRingGroups) {
        TestSyncQuorum({ {.State = PRIMARY}, {.State = SYNCHRONIZED} });
    }

    Y_UNIT_TEST(OneDisconnectedRingGroup) {
        TestSyncQuorum({ {.State = PRIMARY}, {.State = DISCONNECTED} });
    }

    Y_UNIT_TEST(OneSynchronizedRingGroup) {
        TestSyncQuorum({ {.State = PRIMARY}, {.State = SYNCHRONIZED} });
    }

    Y_UNIT_TEST(OneWriteOnlyRingGroup) {
        TestSyncQuorum({ {.State = PRIMARY}, {.State = PRIMARY, .WriteOnly = true} });
    }

    Y_UNIT_TEST(ReplicaConfigMismatch) {
        TTestBasicRuntime runtime;
        SetupMinimalRuntime(runtime);

        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NLog::PRI_DEBUG);

        const auto stateStorageInfo = GetStateStorageInfo(runtime);
        const auto participatingReplicas = CountParticipatingReplicas(*stateStorageInfo);

        constexpr int DomainId = 1;
        constexpr const char* Path = "TestPath";
        const TActorId edge = runtime.AllocateEdgeActor();

        const TActorId subscriber = runtime.Register(CreateSchemeBoardSubscriber(edge, Path, DomainId));
        TBlockEvents<NInternalEvents::TEvNotify> notificationBlocker(runtime, [&](const NInternalEvents::TEvNotify::TPtr& ev) {
            return ev->Recipient == subscriber;
        });
        runtime.WaitFor("initial path lookups", [&]() {
            return notificationBlocker.size() == participatingReplicas;
        }, TDuration::Seconds(10));
        notificationBlocker.Stop().Unblock();

        const auto requiredReplicas = GetReplicasRequiredForQuorum(stateStorageInfo->RingGroups);
        for (const auto& replica : stateStorageInfo->SelectAllReplicas()) {
            if (!FindPtr(requiredReplicas, replica)) {
                Cerr << "Poisoning replica: " << replica << '\n';
                runtime.Send(replica, edge, new TEvents::TEvPoisonPill());
            }
        }

        ui64 cookie = 12345;
        {
            runtime.Send(new IEventHandle(subscriber, edge, new NInternalEvents::TEvSyncRequest(), 0, cookie));
            const auto syncResponse = runtime.GrabEdgeEvent<NInternalEvents::TEvSyncResponse>(edge);

            UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Path, Path, syncResponse->ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Partial, false, syncResponse->ToString());
        }

        {
            const auto replica = requiredReplicas[RandomNumber(requiredReplicas.size())];

            auto newConfig = MakeIntrusive<TStateStorageInfo>(*stateStorageInfo);
            newConfig->ClusterStateGeneration++;
            Cerr << "Updating cluster state generation on replica: " << replica << '\n';
            runtime.Send(replica, edge, new TEvStateStorage::TEvUpdateGroupConfig(nullptr, nullptr, newConfig));

            ++cookie;
            runtime.Send(new IEventHandle(subscriber, edge, new NInternalEvents::TEvSyncRequest(), 0, cookie));
            const auto syncResponse = runtime.GrabEdgeEvent<NInternalEvents::TEvSyncResponse>(edge);

            UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Path, Path, syncResponse->ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Partial, true, syncResponse->ToString());
        }
    }

    Y_UNIT_TEST(ReconfigurationWithDelayedSyncRequest) {
        TTestBasicRuntime runtime;
        SetupMinimalRuntime(runtime);

        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NLog::PRI_DEBUG);

        const auto stateStorageInfo = GetStateStorageInfo(runtime);
        const auto participatingReplicas = CountParticipatingReplicas(*stateStorageInfo);

        constexpr int DomainId = 1;
        constexpr const char* Path = "TestPath";
        const TActorId edge = runtime.AllocateEdgeActor();

        const TActorId subscriber = runtime.Register(CreateSchemeBoardSubscriber(edge, Path, DomainId));
        TBlockEvents<NInternalEvents::TEvNotify> notificationBlocker(runtime, [&](const NInternalEvents::TEvNotify::TPtr& ev) {
            return ev->Recipient == subscriber;
        });
        runtime.WaitFor("initial path lookups", [&]() {
            return notificationBlocker.size() == participatingReplicas;
        }, TDuration::Seconds(10));

        // Send sync request: subscriber will queue it in DelayedSyncRequest since it cannot process syncs before finishing its initialization.
        constexpr ui64 cookie = 12345;
        runtime.Send(new IEventHandle(subscriber, edge, new NInternalEvents::TEvSyncRequest(), 0, cookie));

        auto replicas = ResolveReplicas(runtime, Path);
        runtime.Send(subscriber, edge, replicas->Release().Release());

        // Now allow all notifications through so that initialization completes.
        notificationBlocker.Stop().Unblock();

        auto syncResponse = runtime.GrabEdgeEvent<NInternalEvents::TEvSyncResponse>(edge, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Path, Path, syncResponse->ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Cookie, cookie, syncResponse->ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Partial, false, syncResponse->ToString());

        // No additional sync responses.
        UNIT_CHECK_GENERATED_EXCEPTION(
            runtime.GrabEdgeEvent<NInternalEvents::TEvSyncResponse>(edge, TDuration::Seconds(10)),
            TEmptyEventQueueException
        );
    }

    Y_UNIT_TEST(ReconfigurationWithCurrentSyncRequest) {
        TTestBasicRuntime runtime;
        SetupMinimalRuntime(runtime);

        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NLog::PRI_DEBUG);

        const auto stateStorageInfo = GetStateStorageInfo(runtime);
        const auto participatingReplicas = CountParticipatingReplicas(*stateStorageInfo);

        constexpr int DomainId = 1;
        constexpr const char* Path = "TestPath";
        const TActorId edge = runtime.AllocateEdgeActor();

        const TActorId subscriber = runtime.Register(CreateSchemeBoardSubscriber(edge, Path, DomainId));
        TBlockEvents<NInternalEvents::TEvNotify> notificationBlocker(runtime, [&](const NInternalEvents::TEvNotify::TPtr& ev) {
            return ev->Recipient == subscriber;
        });
        runtime.WaitFor("initial path lookups", [&]() {
            return notificationBlocker.size() == participatingReplicas;
        }, TDuration::Seconds(10));
        notificationBlocker.Stop().Unblock();

        constexpr ui64 cookie = 12345;
        TBlockEvents<NInternalEvents::TEvSyncVersionResponse> syncResponseBlocker(runtime, [&](const NInternalEvents::TEvSyncVersionResponse::TPtr& ev) {
            return ev->Recipient == subscriber && ev->Cookie == cookie;
        });
        runtime.Send(new IEventHandle(subscriber, edge, new NInternalEvents::TEvSyncRequest(), 0, cookie));
        runtime.WaitFor("some sync responses", [&]() {
            return !syncResponseBlocker.empty();
        }, TDuration::Seconds(10));
        syncResponseBlocker.Unblock(1);

        auto replicas = ResolveReplicas(runtime, Path);
        runtime.Send(subscriber, edge, replicas->Release().Release());
        syncResponseBlocker.Stop().Unblock();

        auto syncResponse = runtime.GrabEdgeEvent<NInternalEvents::TEvSyncResponse>(edge, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Path, Path, syncResponse->ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Cookie, cookie, syncResponse->ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(syncResponse->Get()->Partial, false, syncResponse->ToString());

        // No additional sync responses.
        UNIT_CHECK_GENERATED_EXCEPTION(
            runtime.GrabEdgeEvent<NInternalEvents::TEvSyncResponse>(edge, TDuration::Seconds(10)),
            TEmptyEventQueueException
        );
    }
}

} // NSchemeBoard
} // NKikimr
