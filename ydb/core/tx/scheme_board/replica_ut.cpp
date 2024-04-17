#include "replica.h"
#include "ut_helpers.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <ydb/library/actors/core/log.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NSchemeBoard {

namespace {

    TActorId CreateReplica(TTestActorRuntimeBase& runtime) {
        const auto replica = runtime.Register(CreateSchemeBoardReplica(TIntrusivePtr<TStateStorageInfo>(), 0));
        runtime.EnableScheduleForActor(replica, true);

        // wait until replica is ready
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&replica](IEventHandle& ev) {
            return ev.Recipient == replica && ev.GetTypeRewrite() == TEvents::TSystem::Bootstrap;
        });
        runtime.DispatchEvents(opts);

        return replica;
    }

} // anonymous

class TReplicaTest: public NUnitTest::TTestBase {
public:
    void SetUp() override {
        Context = MakeHolder<TTestContext>();
        Context->Initialize(TAppPrepare().Unwrap());
        Context->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NLog::PRI_DEBUG);

        Replica = CreateReplica(*Context);
    }

    void TearDown() override {
        Context.Reset();
    }

    UNIT_TEST_SUITE(TReplicaTest);
    UNIT_TEST(Handshake);
    UNIT_TEST(HandshakeWithStaleGeneration);
    UNIT_TEST(Commit);
    UNIT_TEST(CommitWithoutHandshake);
    UNIT_TEST(CommitWithStaleGeneration);
    UNIT_TEST(Update);
    UNIT_TEST(Delete);
    UNIT_TEST(UpdateWithoutHandshake);
    UNIT_TEST(UpdateWithStaleGeneration);
    UNIT_TEST(Subscribe);
    UNIT_TEST(SubscribeUnknownPath);
    UNIT_TEST(Unsubscribe);
    UNIT_TEST(UnsubscribeUnknownPath);
    UNIT_TEST(DoubleUnsubscribe);
    UNIT_TEST(UnsubscribeWithoutSubscribe);
    UNIT_TEST(Merge);
    UNIT_TEST(DoubleDelete);
    UNIT_TEST(SyncVersion);
    UNIT_TEST(IdempotencyUpdatesAliveSubscriber);
    UNIT_TEST(IdempotencyUpdatesWithoutSubscribers);
    UNIT_TEST(IdempotencyUpdatesVariant2);
    UNIT_TEST(AckNotifications);
    UNIT_TEST(AckNotificationsUponPathRecreation);
    UNIT_TEST(StrongNotificationAfterCommit);
    UNIT_TEST_SUITE_END();

    void Handshake();
    void HandshakeWithStaleGeneration();
    void Commit();
    void CommitWithoutHandshake();
    void CommitWithStaleGeneration();
    void Update();
    void Delete();
    void UpdateWithoutHandshake();
    void UpdateWithStaleGeneration();
    void Subscribe();
    void SubscribeUnknownPath();
    void Unsubscribe();
    void UnsubscribeUnknownPath();
    void DoubleUnsubscribe();
    void UnsubscribeWithoutSubscribe();
    void Merge();
    void DoubleDelete();
    void SyncVersion();
    void IdempotencyUpdates(bool aliveSubscriber);
    void IdempotencyUpdatesAliveSubscriber();
    void IdempotencyUpdatesWithoutSubscribers();
    void IdempotencyUpdatesVariant2();
    void AckNotifications();
    void AckNotificationsUponPathRecreation();
    void StrongNotificationAfterCommit();

private:
    THolder<TTestContext> Context;
    TActorId Replica;

}; // TReplicaTest

UNIT_TEST_SUITE_REGISTRATION(TReplicaTest);

void TReplicaTest::Handshake() {
    auto ev = Context->HandshakeReplica(Replica, Context->AllocateEdgeActor(), 1, 1);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT_VALUES_EQUAL(1, ev->Get()->Record.GetOwner());
    UNIT_ASSERT_VALUES_EQUAL(0, ev->Get()->Record.GetGeneration());
}

void TReplicaTest::HandshakeWithStaleGeneration() {
    const TActorId edge = Context->AllocateEdgeActor();

    Context->HandshakeReplica(Replica, edge, 1, 2);
    Context->HandshakeReplica(Replica, edge, 1, 1, false);

    ui64 counter = Context->CountEdgeEvents<NInternalEvents::TEvHandshakeResponse>();
    UNIT_ASSERT_VALUES_EQUAL(0, counter);
}

void TReplicaTest::Commit() {
    const TActorId edge = Context->AllocateEdgeActor();

    Context->HandshakeReplica(Replica, edge, 1, 1);
    Context->CommitReplica(Replica, edge, 1, 1);

    auto ev = Context->HandshakeReplica(Replica, edge, 1, 2);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT_VALUES_EQUAL(1, ev->Get()->Record.GetOwner());
    UNIT_ASSERT_VALUES_EQUAL(1, ev->Get()->Record.GetGeneration());
}

void TReplicaTest::CommitWithoutHandshake() {
    const TActorId edge = Context->AllocateEdgeActor();

    Context->CommitReplica(Replica, edge, 1, 1);
    auto ev = Context->HandshakeReplica(Replica, edge, 1, 1);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT_VALUES_EQUAL(1, ev->Get()->Record.GetOwner());
    UNIT_ASSERT_VALUES_EQUAL(0, ev->Get()->Record.GetGeneration());
}

void TReplicaTest::CommitWithStaleGeneration() {
    const TActorId edgeA = Context->AllocateEdgeActor();
    const TActorId edgeB = Context->AllocateEdgeActor();

    Context->HandshakeReplica(Replica, edgeA, 1, 0);
    Context->HandshakeReplica(Replica, edgeB, 1, 1);

    Context->CommitReplica(Replica, edgeB, 1, 1);
    Context->CommitReplica(Replica, edgeA, 1, 0);

    auto ev = Context->HandshakeReplica(Replica, edgeA, 1, 2);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT_VALUES_EQUAL(1, ev->Get()->Record.GetOwner());
    UNIT_ASSERT_VALUES_EQUAL(1, ev->Get()->Record.GetGeneration());
}

void TReplicaTest::Update() {
    const TActorId edge = Context->AllocateEdgeActor();

    Context->HandshakeReplica(Replica, edge, 1, 1);
    Context->Send(Replica, edge, GenerateUpdate(GenerateDescribe("path", TPathId(1, 1)), 1, 1));

    auto checks = [](const auto& record) {
        UNIT_ASSERT_VALUES_EQUAL(1, record.GetPathOwnerId());
        UNIT_ASSERT_VALUES_EQUAL("path", record.GetPath());
        UNIT_ASSERT_VALUES_EQUAL(1, record.GetLocalPathId());
        UNIT_ASSERT_VALUES_EQUAL(false, record.GetIsDeletion());
        UNIT_ASSERT_VALUES_EQUAL(1, record.GetVersion());
    };

    {
        auto ev = Context->SubscribeReplica(Replica, edge, "path");
        checks(ev->Get()->GetRecord());
        Context->UnsubscribeReplica(Replica, edge, "path");
    }

    {
        auto ev = Context->SubscribeReplica(Replica, edge, TPathId(1, 1));
        checks(ev->Get()->GetRecord());
        Context->UnsubscribeReplica(Replica, edge, TPathId(1, 1));
    }
}

void TReplicaTest::Delete() {
    auto describe = GenerateDescribe("path", TPathId(42, 1));

    const TActorId edge = Context->AllocateEdgeActor();

    Context->HandshakeReplica(Replica, edge, 1, 1);
    Context->Send(Replica, edge, GenerateUpdate(describe, 1, 1));

    {
        auto ev = Context->SubscribeReplica(Replica, Context->AllocateEdgeActor(), "path");
        UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->GetRecord().GetPath());
        UNIT_ASSERT_VALUES_EQUAL(false, ev->Get()->GetRecord().GetIsDeletion());
    }

    {
        auto ev = Context->SubscribeReplica(Replica, Context->AllocateEdgeActor(), TPathId(42, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, ev->Get()->GetRecord().GetLocalPathId());
        UNIT_ASSERT_VALUES_EQUAL(false, ev->Get()->GetRecord().GetIsDeletion());
    }

    Context->Send(Replica, edge, GenerateUpdate(describe, 1, 1, true));

    {
        auto ev = Context->SubscribeReplica(Replica, Context->AllocateEdgeActor(), "path");
        UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->GetRecord().GetPath());
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetIsDeletion());
        UNIT_ASSERT_VALUES_EQUAL(Max<ui64>(), ev->Get()->GetRecord().GetVersion());
    }

    {
        auto ev = Context->SubscribeReplica(Replica, Context->AllocateEdgeActor(), TPathId(42, 1));
        UNIT_ASSERT_VALUES_EQUAL(1, ev->Get()->GetRecord().GetLocalPathId());
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetIsDeletion());
        UNIT_ASSERT_VALUES_EQUAL(Max<ui64>(), ev->Get()->GetRecord().GetVersion());
    }

    {
        auto ev = Context->SubscribeReplica(Replica, Context->AllocateEdgeActor(), "path");
        UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->GetRecord().GetPath());
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetIsDeletion());
        UNIT_ASSERT_VALUES_EQUAL(Max<ui64>(), ev->Get()->GetRecord().GetVersion());
    }

}

void TReplicaTest::UpdateWithoutHandshake() {
    const TActorId edge = Context->AllocateEdgeActor();

    Context->Send(Replica, edge, GenerateUpdate(GenerateDescribe("path", TPathId(1, 1)), 1, 1));

    {
        auto ev = Context->SubscribeReplica(Replica, edge, "path");
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetIsDeletion());
        Context->UnsubscribeReplica(Replica, edge, "path");
    }

    {
        auto ev = Context->SubscribeReplica(Replica, edge, TPathId(1, 1));
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetIsDeletion());
        Context->UnsubscribeReplica(Replica, edge, TPathId(1, 1));
    }
}

void TReplicaTest::UpdateWithStaleGeneration() {
    const TActorId edge = Context->AllocateEdgeActor();

    Context->HandshakeReplica(Replica, edge, 1, 1);
    Context->Send(Replica, edge, GenerateUpdate(GenerateDescribe("path", TPathId(1, 1)), 1, 0));

    {
        auto ev = Context->SubscribeReplica(Replica, edge, "path");
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetIsDeletion());
        Context->UnsubscribeReplica(Replica, edge, "path");
    }

    {
        auto ev = Context->SubscribeReplica(Replica, edge, TPathId(1, 1));
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetIsDeletion());
        Context->UnsubscribeReplica(Replica, edge, TPathId(1, 1));
    }
}

void TReplicaTest::Subscribe() {
    auto describe = GenerateDescribe("path", TPathId(1, 1));

    const TActorId edge = Context->AllocateEdgeActor();

    Context->HandshakeReplica(Replica, edge, 1, 1);
    Context->Send(Replica, edge, GenerateUpdate(describe, 1, 1));

    Context->SubscribeReplica(Replica, edge, "path", false);
    {
        auto ev = Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(edge);
        UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->GetRecord().GetPath());
        UNIT_ASSERT_VALUES_EQUAL(false, ev->Get()->GetRecord().GetIsDeletion());
    }

    Context->Send(Replica, edge, GenerateUpdate(describe, 1, 1, true));
    {
        auto ev = Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(edge);
        UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->GetRecord().GetPath());
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetIsDeletion());
    }
}

void TReplicaTest::SubscribeUnknownPath() {
    const TActorId edge = Context->AllocateEdgeActor();

    Context->SubscribeReplica(Replica, edge, "path", false);
    {
        auto ev = Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(edge);
        UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->GetRecord().GetPath());
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetIsDeletion());
    }
}

void TReplicaTest::Unsubscribe() {
    auto describe = GenerateDescribe("path", TPathId(1, 1));

    const TActorId edge = Context->AllocateEdgeActor();

    const TActorId subscriberA = Context->AllocateEdgeActor();
    const TActorId subscriberB = Context->AllocateEdgeActor();

    Context->HandshakeReplica(Replica, edge, 1, 1);

    Context->SubscribeReplica(Replica, subscriberA, "path");
    Context->SubscribeReplica(Replica, subscriberB, "path");

    Context->Send(Replica, edge, GenerateUpdate(describe, 1, 1));
    ui64 counter = Context->CountEdgeEvents<NInternalEvents::TEvNotify>();
    UNIT_ASSERT_VALUES_EQUAL(2, counter);

    Context->UnsubscribeReplica(Replica, subscriberA, "path");

    Context->Send(Replica, edge, GenerateUpdate(describe, 1, 1, true));
    counter = Context->CountEdgeEvents<NInternalEvents::TEvNotify>();
    UNIT_ASSERT_VALUES_EQUAL(1, counter);
}

void TReplicaTest::UnsubscribeUnknownPath() {
    const TActorId edge = Context->AllocateEdgeActor();

    Context->UnsubscribeReplica(Replica, edge, "path");
}

void TReplicaTest::DoubleUnsubscribe() {
    auto describe = GenerateDescribe("path", TPathId(1, 1));

    const TActorId edge = Context->AllocateEdgeActor();

    Context->HandshakeReplica(Replica, edge, 1, 1);
    Context->Send(Replica, edge, GenerateUpdate(describe, 1, 1));

    auto ev = Context->SubscribeReplica(Replica, edge, "path", true);
    UNIT_ASSERT_VALUES_EQUAL("path", ev->Get()->GetRecord().GetPath());
    UNIT_ASSERT_VALUES_EQUAL(false, ev->Get()->GetRecord().GetIsDeletion());

    Context->UnsubscribeReplica(Replica, edge, "path");
    Context->UnsubscribeReplica(Replica, edge, "path");
}

void TReplicaTest::UnsubscribeWithoutSubscribe() {
    auto describe = GenerateDescribe("path", TPathId(1, 1));

    const TActorId edge = Context->AllocateEdgeActor();

    Context->HandshakeReplica(Replica, edge, 1, 1);
    Context->Send(Replica, edge, GenerateUpdate(describe, 1, 1));

    Context->UnsubscribeReplica(Replica, edge, "path");
}

void TReplicaTest::Merge() {
    auto describe = GenerateDescribe("path", TPathId(1, 1));

    const TActorId populator = Context->AllocateEdgeActor();
    const TActorId subscriberA = Context->AllocateEdgeActor();
    const TActorId subscriberB = Context->AllocateEdgeActor();

    Context->SubscribeReplica(Replica, subscriberA, "path");
    Context->SubscribeReplica(Replica, subscriberB, TPathId(1, 1));

    Context->HandshakeReplica(Replica, populator);

    Context->Send(Replica, populator, GenerateUpdate(describe));
    Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriberA);
    Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriberB);

    Context->Send(Replica, populator, GenerateUpdate(describe, 1, 1, true));
    Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriberA);
    Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriberB);
}

void TReplicaTest::DoubleDelete() {
    auto describe = GenerateDescribe("path", TPathId(1, 1));

    const TActorId populator = Context->AllocateEdgeActor();
    Context->HandshakeReplica(Replica, populator);

    const TActorId subscriberA = Context->AllocateEdgeActor();
    Context->SubscribeReplica(Replica, subscriberA, "path");

    Context->Send(Replica, populator, GenerateUpdate(describe));
    Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriberA);

    Context->Send(Replica, populator, GenerateUpdate(describe, 1, 1, true));
    Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriberA);

    const TActorId subscriberB = Context->AllocateEdgeActor();
    Context->SubscribeReplica(Replica, subscriberB, "path");

    Context->Send(Replica, populator, GenerateUpdate(describe, 1, 1, true));
}

void TReplicaTest::SyncVersion() {
    const TActorId edge = Context->AllocateEdgeActor();
    const ui64 version = 100500;

    Context->HandshakeReplica(Replica, edge);
    Context->Send(Replica, edge, GenerateUpdate(GenerateDescribe("path", TPathId(1, 1), version)));

    Context->SubscribeReplica(Replica, edge, "path");
    Context->Send(Replica, edge, new NInternalEvents::TEvSyncVersionRequest("path"), 0, 1);
    auto ev = Context->GrabEdgeEvent<NInternalEvents::TEvSyncVersionResponse>(edge);

    UNIT_ASSERT_VALUES_EQUAL(version, ev->Get()->Record.GetVersion());
    UNIT_ASSERT_VALUES_EQUAL(false, ev->Get()->Record.GetPartial());
}

void TReplicaTest::IdempotencyUpdates(bool aliveSubscriber) {
    auto describeA = GenerateDescribe("path", TPathId(1, 1));
    auto describeB = GenerateDescribe("path", TPathId(1, 2)); // same path but another path id

    const TActorId populator = Context->AllocateEdgeActor();
    Context->HandshakeReplica(Replica, populator);

    const TActorId subscriberA = Context->AllocateEdgeActor();
    Context->SubscribeReplica(Replica, subscriberA, TPathId(1, 1));

    Context->Send(Replica, populator, GenerateUpdate(describeA));
    Context->Send(Replica, populator, GenerateUpdate(describeA, 1, 1, true));

    if (!aliveSubscriber) {
        Context->UnsubscribeReplica(Replica, subscriberA, TPathId(1, 1));
    }

    Context->Send(Replica, populator, GenerateUpdate(describeA));
    Context->Send(Replica, populator, GenerateUpdate(describeB));

    const TActorId subscriberB = Context->AllocateEdgeActor();
    Context->SubscribeReplica(Replica, subscriberB, TPathId(1, 2));
}

void TReplicaTest::IdempotencyUpdatesAliveSubscriber() {
    IdempotencyUpdates(true);
}

void TReplicaTest::IdempotencyUpdatesWithoutSubscribers() {
    IdempotencyUpdates(false);
}

void TReplicaTest::IdempotencyUpdatesVariant2() {
    auto describeA = GenerateDescribe("path", TPathId(1, 1));
    auto describeB = GenerateDescribe("path", TPathId(1, 2)); // same path but another path id

    const TActorId populator = Context->AllocateEdgeActor();
    Context->HandshakeReplica(Replica, populator);

    Context->Send(Replica, populator, GenerateUpdate(describeA));
    Context->Send(Replica, populator, GenerateUpdate(describeB));
    Context->Send(Replica, populator, GenerateUpdate(describeB, 1, 1, true));
    Context->Send(Replica, populator, GenerateUpdate(describeA));
    Context->Send(Replica, populator, GenerateUpdate(describeB));
}

void TReplicaTest::AckNotifications() {
    auto describe = GenerateDescribe("path", TPathId(1, 1));

    const TActorId populator = Context->AllocateEdgeActor();
    const TActorId subscriber = Context->AllocateEdgeActor();

    NKikimrSchemeBoard::TEvSubscribe::TCapabilities capabilities;
    capabilities.SetAckNotifications(true);
    Context->SubscribeReplica(Replica, subscriber, "path", true, 0, capabilities);

    Context->HandshakeReplica(Replica, populator);

    Context->Send(Replica, populator, GenerateUpdate(describe));
    Context->Send(Replica, subscriber, new NInternalEvents::TEvNotifyAck(0));
    Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriber);

    Context->Send(Replica, populator, GenerateUpdate(describe, 1, 1, true));
    Context->Send(Replica, subscriber, new NInternalEvents::TEvNotifyAck(1));
    Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriber);
}

void TReplicaTest::AckNotificationsUponPathRecreation() {
    const TActorId populator = Context->AllocateEdgeActor();
    Context->HandshakeReplica(Replica, populator);

    // initial path version is 2
    Context->Send(Replica, populator, GenerateUpdate(GenerateDescribe("path", TPathId(1, 1), 2)));

    const TActorId subscriber = Context->AllocateEdgeActor();
    NKikimrSchemeBoard::TEvSubscribe::TCapabilities capabilities;
    capabilities.SetAckNotifications(true);
    Context->SubscribeReplica(Replica, subscriber, "path", true, 0, capabilities);

    // update to version 3 & omit ack
    Context->Send(Replica, populator, GenerateUpdate(GenerateDescribe("path", TPathId(1, 1), 3)));
    {
        auto ev = Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(1, ev->Get()->GetRecord().GetLocalPathId());
        UNIT_ASSERT_VALUES_EQUAL(3, ev->Get()->GetRecord().GetVersion());
    }

    // recreate path with version 1
    Context->Send(Replica, populator, GenerateUpdate(GenerateDescribe("path", TPathId(1, 2), 1)));
    // ack previous version
    Context->Send(Replica, subscriber, new NInternalEvents::TEvNotifyAck(3));
    // should receive notification about recreated path
    while (true) {
        auto ev = Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriber);

        if (ev->Get()->GetRecord().GetIsDeletion()) {
            // deletion of previous version
            UNIT_ASSERT_VALUES_EQUAL(1, ev->Get()->GetRecord().GetLocalPathId());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(2, ev->Get()->GetRecord().GetLocalPathId());
            UNIT_ASSERT_VALUES_EQUAL(1, ev->Get()->GetRecord().GetVersion());
            break;
        }

        Context->Send(Replica, subscriber, new NInternalEvents::TEvNotifyAck(ev->Get()->GetRecord().GetVersion()));
    }
}

void TReplicaTest::StrongNotificationAfterCommit() {
    const TActorId populator = Context->AllocateEdgeActor();
    const TActorId subscriber = Context->AllocateEdgeActor();

    Context->SubscribeReplica(Replica, subscriber, "path", false, 1);
    {
        auto ev = Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetIsDeletion());
        UNIT_ASSERT_VALUES_EQUAL(false, ev->Get()->GetRecord().GetStrong());
    }

    Context->HandshakeReplica(Replica, populator);
    Context->CommitReplica(Replica, populator, 1, 1);
    {
        auto ev = Context->GrabEdgeEvent<NInternalEvents::TEvNotify>(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetIsDeletion());
        UNIT_ASSERT_VALUES_EQUAL(true, ev->Get()->GetRecord().GetStrong());
    }
}

class TReplicaCombinationTest: public NUnitTest::TTestBase {
public:
    void SetUp() override {
        Context = MakeHolder<TTestContext>();
        Context->Initialize(TAppPrepare().Unwrap());
        Context->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NLog::PRI_DEBUG);
        Context->SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NLog::PRI_DEBUG);
    }

    TActorId GetReplica() {
        return CreateReplica(*Context);
    }

    void TearDown() override {
        Context.Reset();
    }

    UNIT_TEST_SUITE(TReplicaCombinationTest);
    UNIT_TEST(UpdatesCombinationsDomainRoot);
    UNIT_TEST(UpdatesCombinationsMigratedPath);
    UNIT_TEST(MigratedPathRecreation);
    UNIT_TEST_SUITE_END();

    void UpdatesCombinationsDomainRoot();
    void MigratedPathRecreation();
    void UpdatesCombinationsMigratedPath();


private:
    THolder<TTestContext> Context;
}; // TReplicaCombinationTest

UNIT_TEST_SUITE_REGISTRATION(TReplicaCombinationTest);

void TReplicaCombinationTest::UpdatesCombinationsDomainRoot() {
    ui64 gssOwnerID = 800;
    TVector<TCombinationsArgs> combinations = GenerateCombinationsDomainRoot("/Root/Tenant", gssOwnerID);

    //make all the variants
    for (const auto& argsLeft: combinations) {
        for (const auto& argsRight: combinations) {
            const TActorId replica = GetReplica();

            const TActorId populatorLeft = Context->AllocateEdgeActor();
            Context->HandshakeReplica(replica, populatorLeft, argsLeft.OwnerId, argsLeft.Generation);
            Context->CommitReplica(replica, populatorLeft, argsLeft.OwnerId, argsLeft.Generation);

            const TActorId populatorRight = Context->AllocateEdgeActor();
            Context->HandshakeReplica(replica, populatorRight, argsRight.OwnerId, argsRight.Generation);
            Context->CommitReplica(replica, populatorRight, argsRight.OwnerId, argsRight.Generation);

            Context->Send(replica, populatorLeft, argsLeft.GenerateUpdate());

            Context->Send(replica, populatorRight, argsRight.GenerateUpdate());

            TCombinationsArgs::TSuperId winId = Max(argsLeft.GetSuperId(), argsRight.GetSuperId());

            auto finalSubscriber = Context->AllocateEdgeActor();
            auto ev = Context->SubscribeReplica(replica, finalSubscriber, "/Root/Tenant");

            Cerr << "=========== " << argsLeft.GenerateDescribe().ShortDebugString()
                 << "\n=========== " << argsRight.GenerateDescribe().ShortDebugString() << Endl;

            // KIKIMR-9236#5e70e0b141a0f221ddada730
            if (argsLeft.PathId == TPathId(800, 2) && !argsLeft.IsDeletion && argsLeft.DomainId == TPathId(800, 2)
                && argsRight.PathId == TPathId(800, 3) && argsRight.IsDeletion && argsRight.DomainId == TPathId(800, 3))
            {
                // 1 this is bug, skip it
                continue;
            }

            if (argsLeft.PathId == TPathId(900, 1) && !argsLeft.IsDeletion && argsLeft.DomainId == TPathId(800, 2)
                && argsRight.PathId == TPathId(800, 2) && argsRight.IsDeletion && argsRight.DomainId == TPathId(800, 2))
            {
                // 2 this is bug, skip it
                continue;
            }

            if (argsLeft.PathId == TPathId(900, 1) && !argsLeft.IsDeletion && argsLeft.DomainId == TPathId(800, 2)
                && argsRight.PathId == TPathId(800, 3) && argsRight.IsDeletion && argsRight.DomainId == TPathId(800, 3))
            {
                // 3 this is bug, skip it
                continue;
            }

            if (argsLeft.PathId == TPathId(800, 2) && argsLeft.IsDeletion && argsLeft.DomainId == TPathId(800, 2)
                && argsRight.PathId == TPathId(900, 1) && !argsRight.IsDeletion && argsRight.DomainId == TPathId(800, 2))
            {
                // 4 this is bug, skip it
                continue;
            }

            if (argsLeft.PathId == TPathId(910, 1) && !argsLeft.IsDeletion && argsLeft.DomainId == TPathId(800, 3)
                && argsRight.PathId == TPathId(gssOwnerID, 3) && argsRight.IsDeletion && argsRight.DomainId == TPathId(800, 3))
            {
                // 5 this is bug, skip it
                continue;
            }

            if (argsLeft.PathId == TPathId(800, 3) && argsLeft.IsDeletion && argsLeft.DomainId == TPathId(800, 3)
                && argsRight.PathId == TPathId(800, 2) && !argsRight.IsDeletion && argsRight.DomainId == TPathId(800, 2))
            {
                // 6 this is bug, skip it
                continue;
            }

            if (argsLeft.PathId == TPathId(800, 3) && argsLeft.IsDeletion && argsLeft.DomainId == TPathId(800, 3)
                && argsRight.PathId == TPathId(900, 1) && !argsRight.IsDeletion && argsRight.DomainId == TPathId(800, 2))
            {
                // 7 this is bug, skip it
                continue;
            }

            if (argsLeft.PathId == TPathId(800, 3) && argsLeft.IsDeletion && argsLeft.DomainId == TPathId(800, 3)
                && argsRight.PathId == TPathId(910, 1) && !argsRight.IsDeletion && argsRight.DomainId == TPathId(800, 3))
            {
                // 8 this is bug, skip it
                continue;
            }



            UNIT_ASSERT_VALUES_EQUAL("/Root/Tenant", ev->Get()->GetRecord().GetPath());
            UNIT_ASSERT_VALUES_EQUAL(std::get<1>(winId), ev->Get()->GetRecord().GetIsDeletion());

            if (!ev->Get()->GetRecord().GetIsDeletion()) {
                UNIT_ASSERT_VALUES_EQUAL(std::get<2>(winId), TPathId(ev->Get()->GetRecord().GetPathOwnerId(), ev->Get()->GetRecord().GetLocalPathId()));
                UNIT_ASSERT_VALUES_EQUAL(std::get<3>(winId), ev->Get()->GetRecord().GetVersion());

                UNIT_ASSERT(ev->Get()->GetRecord().HasDescribeSchemeResultSerialized());
                UNIT_ASSERT(ev->Get()->GetRecord().HasPathSubdomainPathId());

                UNIT_ASSERT_VALUES_EQUAL(std::get<0>(winId), PathIdFromPathId(ev->Get()->GetRecord().GetPathSubdomainPathId()));
            }
        }
    }
}

void TReplicaCombinationTest::MigratedPathRecreation() {
    ui64 gssOwnerID = 800;
    ui64 tssOwnerID = 900;

    auto domainId = TPathId(gssOwnerID, 1);
    auto migratedPathId = TPathId(gssOwnerID, 1111);

    auto recreatedPathId = TPathId(tssOwnerID, 11);

    auto argsLeft = TCombinationsArgs{
            "/root/db/dir_inside", migratedPathId, 1, domainId,
            gssOwnerID, 1, false};
    auto argsRight = TCombinationsArgs{
            "/root/db/dir_inside", recreatedPathId, 1, domainId,
            tssOwnerID, 1, false};


    const TActorId replica = GetReplica();

    const TActorId populatorLeft = Context->AllocateEdgeActor();
    Context->HandshakeReplica(replica, populatorLeft, argsLeft.OwnerId, argsLeft.Generation);
    Context->CommitReplica(replica, populatorLeft, argsLeft.OwnerId, argsLeft.Generation);

    const TActorId populatorRight = Context->AllocateEdgeActor();
    Context->HandshakeReplica(replica, populatorRight, argsRight.OwnerId, argsRight.Generation);
    Context->CommitReplica(replica, populatorRight, argsRight.OwnerId, argsRight.Generation);

    Context->Send(replica, populatorLeft, argsLeft.GenerateUpdate());

    Context->Send(replica, populatorRight, argsRight.GenerateUpdate());

    TCombinationsArgs::TSuperId winId = Max(argsLeft.GetSuperId(), argsRight.GetSuperId());

    auto finalSubscriber = Context->AllocateEdgeActor();
    auto ev = Context->SubscribeReplica(replica, finalSubscriber, "/root/db/dir_inside");

    Cerr << "=========== " << argsLeft.GenerateDescribe().ShortDebugString()
         << "\n=========== " << argsRight.GenerateDescribe().ShortDebugString()
         << "\n==========="
         << " DomainId: " << std::get<0>(winId)
         << " IsDeletion: " << std::get<1>(winId)
         << " PathId: " << std::get<2>(winId)
         << " Versions: " << std::get<3>(winId)
         << Endl;

    UNIT_ASSERT_VALUES_EQUAL("/root/db/dir_inside", ev->Get()->GetRecord().GetPath());
    UNIT_ASSERT_VALUES_EQUAL(std::get<1>(winId), ev->Get()->GetRecord().GetIsDeletion());

    UNIT_ASSERT_VALUES_EQUAL(std::get<2>(winId), TPathId(ev->Get()->GetRecord().GetPathOwnerId(), ev->Get()->GetRecord().GetLocalPathId()));
    UNIT_ASSERT_VALUES_EQUAL(std::get<3>(winId), ev->Get()->GetRecord().GetVersion());

    UNIT_ASSERT(ev->Get()->GetRecord().HasDescribeSchemeResultSerialized());
    UNIT_ASSERT(ev->Get()->GetRecord().HasPathSubdomainPathId());

    UNIT_ASSERT_VALUES_EQUAL(std::get<0>(winId), PathIdFromPathId(ev->Get()->GetRecord().GetPathSubdomainPathId()));
}

void TReplicaCombinationTest::UpdatesCombinationsMigratedPath() {
    ui64 gssID = 800;
    ui64 tssID = 900;
    ui64 tssIDrecreated = 910;
    ui64 gssLocalId = 5;
    ui64 tssLocalId = 9;
    TVector<TCombinationsArgs> combinations = GenerateCombinationsMigratedPath("/Root/Tenant/table_inside", gssID, {tssID, tssIDrecreated}, gssLocalId, tssLocalId);

    //make all the variants
    for (const auto& argsLeft: combinations) {
        for (const auto& argsRight: combinations) {
            const TActorId replica = GetReplica();

            const TActorId populatorLeft = Context->AllocateEdgeActor();
            Context->HandshakeReplica(replica, populatorLeft, argsLeft.OwnerId, argsLeft.Generation);
            Context->CommitReplica(replica, populatorLeft, argsLeft.OwnerId, argsLeft.Generation);

            const TActorId populatorRight = Context->AllocateEdgeActor();
            Context->HandshakeReplica(replica, populatorRight, argsRight.OwnerId, argsRight.Generation);
            Context->CommitReplica(replica, populatorRight, argsRight.OwnerId, argsRight.Generation);

            Context->Send(replica, populatorLeft, argsLeft.GenerateUpdate());

            Context->Send(replica, populatorRight, argsRight.GenerateUpdate());

            TCombinationsArgs::TSuperId winId = Max(argsLeft.GetSuperId(), argsRight.GetSuperId());

            auto finalSubscriber = Context->AllocateEdgeActor();
            auto ev = Context->SubscribeReplica(replica, finalSubscriber, "/Root/Tenant/table_inside");

            Cerr << "=========== Left ==" << argsLeft.GenerateDescribe().ShortDebugString()
                 << "\n=========== Right ==" << argsRight.GenerateDescribe().ShortDebugString()
                 << "\n=========== super id =="
                 << " DomainId: " << std::get<0>(winId)
                 << " IsDeletion: " << std::get<1>(winId)
                 << " PathId: " << std::get<2>(winId)
                 << " Verions: " << std::get<3>(winId)
                 << Endl;
            Cerr << "=========== WIN =="
                 << ev->Get()->GetRecord().GetPath()
                 << " PathID: " <<  TPathId(ev->Get()->GetRecord().GetPathOwnerId(), ev->Get()->GetRecord().GetLocalPathId())
                 << " deleted: " <<  ev->Get()->GetRecord().GetIsDeletion()
                 << " version: " << ev->Get()->GetRecord().GetVersion()
                 << " domainId: " << PathIdFromPathId(ev->Get()->GetRecord().GetPathSubdomainPathId())
                 << Endl;

            if (argsLeft.PathId == TPathId(gssID, gssLocalId) && !argsLeft.IsDeletion && argsLeft.DomainId == TPathId(gssID, 2)
                && argsRight.PathId == TPathId(tssID, tssLocalId) && argsRight.IsDeletion && argsRight.DomainId == TPathId(gssID, 2))
            {
                // 1 this is bug
                continue;
            }

            if (argsLeft.PathId == TPathId(gssID, gssLocalId) && argsLeft.IsDeletion && argsLeft.DomainId == TPathId(gssID, 2)
              && argsRight.PathId == TPathId(tssID, tssLocalId) && !argsRight.IsDeletion && argsRight.DomainId == TPathId(gssID, 2))
            {
                // 2 this is NOT bug, this is super id violation

                UNIT_ASSERT_VALUES_EQUAL("/Root/Tenant/table_inside", ev->Get()->GetRecord().GetPath());
                UNIT_ASSERT_VALUES_EQUAL(std::get<1>(argsRight.GetSuperId()), ev->Get()->GetRecord().GetIsDeletion());

                UNIT_ASSERT_VALUES_EQUAL(std::get<2>(argsRight.GetSuperId()), TPathId(ev->Get()->GetRecord().GetPathOwnerId(), ev->Get()->GetRecord().GetLocalPathId()));
                UNIT_ASSERT_VALUES_EQUAL(std::get<3>(argsRight.GetSuperId()), ev->Get()->GetRecord().GetVersion());

                UNIT_ASSERT(ev->Get()->GetRecord().HasDescribeSchemeResultSerialized());
                UNIT_ASSERT(ev->Get()->GetRecord().HasPathSubdomainPathId());

                UNIT_ASSERT_VALUES_EQUAL(std::get<0>(argsRight.GetSuperId()), PathIdFromPathId(ev->Get()->GetRecord().GetPathSubdomainPathId()));

                continue;
            }

            if (argsLeft.PathId == TPathId(tssID, tssLocalId) && !argsLeft.IsDeletion && argsLeft.DomainId == TPathId(gssID, 2)
                && argsRight.PathId == TPathId(gssID, gssLocalId) && argsRight.IsDeletion && argsRight.DomainId == TPathId(gssID, 2))
            {
                // 3 this is NOT bug, this is super id violation

                UNIT_ASSERT_VALUES_EQUAL("/Root/Tenant/table_inside", ev->Get()->GetRecord().GetPath());
                UNIT_ASSERT_VALUES_EQUAL(std::get<1>(argsLeft.GetSuperId()), ev->Get()->GetRecord().GetIsDeletion());

                UNIT_ASSERT_VALUES_EQUAL(std::get<2>(argsLeft.GetSuperId()), TPathId(ev->Get()->GetRecord().GetPathOwnerId(), ev->Get()->GetRecord().GetLocalPathId()));
                UNIT_ASSERT_VALUES_EQUAL(std::get<3>(argsLeft.GetSuperId()), ev->Get()->GetRecord().GetVersion());

                UNIT_ASSERT(ev->Get()->GetRecord().HasDescribeSchemeResultSerialized());
                UNIT_ASSERT(ev->Get()->GetRecord().HasPathSubdomainPathId());

                UNIT_ASSERT_VALUES_EQUAL(std::get<0>(argsLeft.GetSuperId()), PathIdFromPathId(ev->Get()->GetRecord().GetPathSubdomainPathId()));

                continue;
            }

            if (argsLeft.PathId == TPathId(tssID, tssLocalId) && argsLeft.IsDeletion && argsLeft.DomainId == TPathId(gssID, 2)
                && argsRight.PathId == TPathId(gssID, gssLocalId) && !argsRight.IsDeletion && argsRight.DomainId == TPathId(gssID, 2))
            {
                // 4 this is bug
                continue;
            }

            if (argsLeft.PathId == TPathId(gssID, gssLocalId) && !argsLeft.IsDeletion && argsLeft.DomainId == TPathId(gssID, 2)
                && argsRight.PathId == TPathId(tssIDrecreated, tssLocalId) && argsRight.IsDeletion && argsRight.DomainId == TPathId(gssID, 333))
            {
                // 5 this is bug
                continue;
            }

            if (argsLeft.PathId == TPathId(tssID, tssLocalId) && !argsLeft.IsDeletion && argsLeft.DomainId == TPathId(gssID, 2)
                && argsRight.PathId == TPathId(tssIDrecreated, tssLocalId) && argsRight.IsDeletion && argsRight.DomainId == TPathId(gssID, 333))
            {
                // 6 this is bug
                continue;
            }

            if (argsLeft.PathId == TPathId(tssIDrecreated, tssLocalId) && argsLeft.IsDeletion && argsLeft.DomainId == TPathId(gssID, 333)
                && argsRight.PathId == TPathId(gssID, gssLocalId) && !argsRight.IsDeletion && argsRight.DomainId == TPathId(gssID, 2))
            {
                // 7 this is bug
                continue;
            }

            if (argsLeft.PathId == TPathId(tssIDrecreated, tssLocalId) && argsLeft.IsDeletion && argsLeft.DomainId == TPathId(gssID, 333)
              && argsRight.PathId == TPathId(tssID, tssLocalId) && !argsRight.IsDeletion && argsRight.DomainId == TPathId(gssID, 2))
            {
                // 8 this is bug
                continue;
            }

            UNIT_ASSERT_VALUES_EQUAL("/Root/Tenant/table_inside", ev->Get()->GetRecord().GetPath());
            UNIT_ASSERT_VALUES_EQUAL(std::get<1>(winId), ev->Get()->GetRecord().GetIsDeletion());

            if (!ev->Get()->GetRecord().GetIsDeletion()) {
                UNIT_ASSERT_VALUES_EQUAL(std::get<2>(winId), TPathId(ev->Get()->GetRecord().GetPathOwnerId(), ev->Get()->GetRecord().GetLocalPathId()));
                UNIT_ASSERT_VALUES_EQUAL(std::get<3>(winId), ev->Get()->GetRecord().GetVersion());

                UNIT_ASSERT(ev->Get()->GetRecord().HasDescribeSchemeResultSerialized());
                UNIT_ASSERT(ev->Get()->GetRecord().HasPathSubdomainPathId());

                UNIT_ASSERT_VALUES_EQUAL(std::get<0>(winId), PathIdFromPathId(ev->Get()->GetRecord().GetPathSubdomainPathId()));
            }
        }
    }
}

} // NSchemeBoard
} // NKikimr
