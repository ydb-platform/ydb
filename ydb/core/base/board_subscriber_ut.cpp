#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/statestorage_impl.h>

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <util/generic/vector.h>
#include <util/generic/xrange.h>

namespace NKikimr {

class TBoardSubscriberTestBase : public NUnitTest::TTestBase {

    TActorId CreateSubscriber(const TString& path, const TActorId& owner, ui32 nodeIdx) {
        const TActorId subscriber = Context->Register(
            CreateBoardLookupActor(path, owner, EBoardLookupMode::Subscription), nodeIdx
        );
        Context->EnableScheduleForActor(subscriber);
        return subscriber;
    }

    TActorId CreateLookup(const TString& path, const TActorId& owner, EBoardLookupMode mode, ui32 nodeIdx) {
        const TActorId lookup = Context->Register(CreateBoardLookupActor(path, owner, mode), nodeIdx);
        Context->EnableScheduleForActor(lookup);
        return lookup;
    }

    TActorId CreatePublisher(
            const TString& path, const TString& payload, const TActorId& owner, ui32 nodeIdx) {
        const TActorId publisher = Context->Register(
            CreateBoardPublishActor(path, payload, owner, 0, true), nodeIdx
        );
        Context->EnableScheduleForActor(publisher);
        return publisher;
    }

    void Send(
            const TActorId& recipient,
            const TActorId& sender,
            IEventBase* ev,
            ui32 nodeIdx,
            bool viaActorSystem = false) {
        Context->Send(new IEventHandle(recipient, sender, ev, 0, 0), nodeIdx, viaActorSystem);
    }

    void KillPublisher(const TActorId& owner, const TActorId& publisher, ui32 nodeIdx) {
        Send(publisher, owner, new TEvents::TEvPoisonPill(), nodeIdx);
    }

    void Disconnect(ui32 nodeIndexFrom, ui32 nodeIndexTo) {
        const TActorId proxy = Context->GetInterconnectProxy(nodeIndexFrom, nodeIndexTo);

        Send(proxy, TActorId(), new TEvInterconnect::TEvDisconnect(), nodeIndexFrom, true);

        //Wait for event TEvInterconnect::EvNodeDisconnected
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
        Context->DispatchEvents(options);
    }

    TVector<TActorId> ResolveReplicas() {
        const TActorId proxy = MakeStateStorageProxyID();
        const TActorId edge = Context->AllocateEdgeActor();

        Context->Send(proxy, edge, new TEvStateStorage::TEvResolveBoard("path"));
        auto ev = Context->GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(edge);

        auto allReplicas = ev->Get()->GetPlainReplicas();
        return TVector<TActorId>(allReplicas.begin(), allReplicas.end());
    }


public:
    void SetUp() override {
        Context = MakeHolder<TTestBasicRuntime>(3);

        SetupCustomStateStorage(*Context, 3, 3, 1);

        Context->Initialize(TAppPrepare().Unwrap());
    }

    void TearDown() override {
        Context.Reset();
    }

    void SimpleSubscriber();
    void ManySubscribersManyPublisher();
    void NotAvailableByShutdown();
    void ReconnectReplica();
    void DropByDisconnect();
    void KeepInfoFromOtherRingGroup();
    void IgnoreWriteOnlyRingGroupOnLookup();
    void ReturnNotAvailableWithoutReadableRingGroup();

protected:
    THolder<TTestBasicRuntime> Context;
};

class TBoardSubscriberTest : public TBoardSubscriberTestBase {
    UNIT_TEST_SUITE(TBoardSubscriberTest);
    UNIT_TEST(SimpleSubscriber);
    UNIT_TEST(ManySubscribersManyPublisher);
    UNIT_TEST(NotAvailableByShutdown);
    UNIT_TEST(ReconnectReplica);
    UNIT_TEST(DropByDisconnect);
    UNIT_TEST_SUITE_END();
};

class TBoardSubscriber2DCTest : public TBoardSubscriberTest {
public:
    void SetUp() override {
        Context = MakeHolder<TTestBasicRuntime>(6);

        SetupCustomStateStorage(*Context, 3, 3, 1, 2);

        Context->Initialize(TAppPrepare().Unwrap());
    }

    UNIT_TEST_SUITE(TBoardSubscriber2DCTest);
    UNIT_TEST(SimpleSubscriber);
    UNIT_TEST(ManySubscribersManyPublisher);
    UNIT_TEST(NotAvailableByShutdown);
    UNIT_TEST(ReconnectReplica);
    UNIT_TEST(DropByDisconnect);
    UNIT_TEST(KeepInfoFromOtherRingGroup);
    UNIT_TEST_SUITE_END();
};

class TBoardLookupWriteOnlyTest : public TBoardSubscriberTestBase {
public:
    void SetUp() override {
        Context = MakeHolder<TTestBasicRuntime>(1);
        auto setup = CreateCustomStateStorageSetupper({
            {.State = PRIMARY},
            {.State = PRIMARY, .WriteOnly = true},
        }, 3);
        setup(*Context, 0);
        Context->Initialize(TAppPrepare().Unwrap());
    }

    UNIT_TEST_SUITE(TBoardLookupWriteOnlyTest);
    UNIT_TEST(IgnoreWriteOnlyRingGroupOnLookup);
    UNIT_TEST_SUITE_END();
};

class TBoardLookupAllWriteOnlyTest : public TBoardSubscriberTestBase {
public:
    void SetUp() override {
        Context = MakeHolder<TTestBasicRuntime>(1);
        auto setup = CreateCustomStateStorageSetupper({
            {.State = PRIMARY, .WriteOnly = true},
        }, 3);
        setup(*Context, 0);
        Context->Initialize(TAppPrepare().Unwrap());
    }

    UNIT_TEST_SUITE(TBoardLookupAllWriteOnlyTest);
    UNIT_TEST(ReturnNotAvailableWithoutReadableRingGroup);
    UNIT_TEST_SUITE_END();
};

UNIT_TEST_SUITE_REGISTRATION(TBoardSubscriber2DCTest);

UNIT_TEST_SUITE_REGISTRATION(TBoardSubscriberTest);
UNIT_TEST_SUITE_REGISTRATION(TBoardLookupWriteOnlyTest);
UNIT_TEST_SUITE_REGISTRATION(TBoardLookupAllWriteOnlyTest);

void TBoardSubscriberTestBase::SimpleSubscriber() {
    const auto edgeSubscriber = Context->AllocateEdgeActor(0);
    CreateSubscriber("path", edgeSubscriber, 0);

    {
        auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfo>(edgeSubscriber);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
        UNIT_ASSERT(event->Get()->InfoEntries.empty());
    }

    const auto edgePublisher = Context->AllocateEdgeActor(1);
    const TActorId publisher = CreatePublisher("path", "test", edgePublisher, 1);

    {
        auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfoUpdate>(edgeSubscriber);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
        const auto &updates = event->Get()->Updates;
        UNIT_ASSERT(updates.size() == 1);

        auto updatesIt = updates.find(publisher);
        UNIT_ASSERT(updatesIt != updates.end());

        UNIT_ASSERT(!updatesIt->second.Dropped);
        UNIT_ASSERT_EQUAL(updatesIt->second.Payload, "test");
    }

    KillPublisher(edgePublisher, publisher, 1);

    {
        auto event = Context->GrabEdgeEventIf<TEvStateStorage::TEvBoardInfoUpdate>(
            {edgeSubscriber}, [edgeSubscriber, publisher](const TEvStateStorage::TEvBoardInfoUpdate::TPtr& ev){
            const auto &updates = ev->Get()->Updates;
            UNIT_ASSERT(updates.size() == 1);
            auto updatesIt = updates.find(publisher);
            UNIT_ASSERT(updatesIt != updates.end());
            if (ev->Recipient == edgeSubscriber && updatesIt->second.Dropped) {
                return true;
            }
            return false;
        });
        UNIT_ASSERT(event);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
        const auto &updates = event->Get()->Updates;

        auto updatesIt = updates.find(publisher);
        UNIT_ASSERT(updatesIt != updates.end());
        UNIT_ASSERT(updatesIt->second.Dropped);
    }
}

void TBoardSubscriberTestBase::ManySubscribersManyPublisher() {
    size_t subscribersCount = 10;
    size_t publishersCount = 10;

    THashSet<TActorId> subscribers;

    for (size_t i = 0; i < subscribersCount; i++) {
        const auto edgeSubscriber = Context->AllocateEdgeActor(i % 3);
        CreateSubscriber("path", edgeSubscriber, i % 3);
        subscribers.insert(edgeSubscriber);
        {
            auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfo>(edgeSubscriber);
            UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
            UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
            UNIT_ASSERT(event->Get()->InfoEntries.empty());
        }
    }

    for (size_t i = 0; i < publishersCount; i++) {
        const auto edgePublisher = Context->AllocateEdgeActor(i % 3);
        const auto publisher = CreatePublisher("path", ToString(i), edgePublisher, i % 3);
        for (const auto& subscriber : subscribers) {
            auto event = Context->GrabEdgeEventIf<TEvStateStorage::TEvBoardInfoUpdate>(
                {subscriber}, [publisher](const TEvStateStorage::TEvBoardInfoUpdate::TPtr& ev) mutable {
                    const auto &updates = ev->Get()->Updates;
                    UNIT_ASSERT(updates.size() == 1);
                    auto updatesIt = updates.find(publisher);
                    if (updatesIt != updates.end()) {
                        return true;
                    }
                    return false;
                });
            UNIT_ASSERT_EQUAL(event->Get()->Path, "path");

            const auto &updates = event->Get()->Updates;
            UNIT_ASSERT(updates.size() == 1);

            auto updatesIt = updates.find(publisher);
            UNIT_ASSERT(updatesIt != updates.end());

            UNIT_ASSERT_EQUAL(updatesIt->second.Payload, ToString(i));
        }
    }
}

void TBoardSubscriberTestBase::NotAvailableByShutdown() {

    auto replicas = ResolveReplicas();

    const auto edgeSubscriber = Context->AllocateEdgeActor(1);
    CreateSubscriber("path", edgeSubscriber, 1);

    {
        auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfo>(edgeSubscriber);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
        UNIT_ASSERT(event->Get()->InfoEntries.empty());
    }

    Send(replicas[0], TActorId(), new TEvents::TEvPoisonPill(), 0, true);
    Send(replicas[1], TActorId(), new TEvents::TEvPoisonPill(), 0, true);


    auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfoUpdate>(edgeSubscriber);
    UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable);
}

void TBoardSubscriberTestBase::ReconnectReplica() {

    const auto edgeSubscriber = Context->AllocateEdgeActor(1);
    CreateSubscriber("path", edgeSubscriber, 1);

    {
        auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfo>(edgeSubscriber);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
        UNIT_ASSERT(event->Get()->InfoEntries.empty());
    }

    Disconnect(1, 0);

    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvStateStorage::EvReplicaBoardInfo);
    Context->DispatchEvents(options);

    Disconnect(1, 2);

    const auto edgePublisher = Context->AllocateEdgeActor(0);
    const TActorId publisher = CreatePublisher("path", "test", edgePublisher, 0);

    {
        auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfoUpdate>(edgeSubscriber);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
        const auto &updates = event->Get()->Updates;
        UNIT_ASSERT(updates.size() == 1);

        auto updatesIt = updates.find(publisher);
        UNIT_ASSERT(updatesIt != updates.end());

        UNIT_ASSERT_EQUAL(updatesIt->second.Payload, "test");
    }
}

void TBoardSubscriberTestBase::DropByDisconnect() {
    auto replicas = ResolveReplicas();

    const auto edgeSubscriber = Context->AllocateEdgeActor(1);
    CreateSubscriber("path", edgeSubscriber, 1);

    {
        auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfo>(edgeSubscriber);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
        UNIT_ASSERT(event->Get()->InfoEntries.empty());
    }

    auto prevObserverFunc = Context->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvStateStorage::TEvReplicaBoardPublish::EventType: {
                if (ev->Recipient != replicas[0]) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

    const auto edgePublisher = Context->AllocateEdgeActor(0);
    const TActorId publisher = CreatePublisher("path", "test", edgePublisher, 0);

    {
        auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfoUpdate>(edgeSubscriber);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
        const auto &updates = event->Get()->Updates;
        UNIT_ASSERT(updates.size() == 1);

        auto updatesIt = updates.find(publisher);
        UNIT_ASSERT(updatesIt != updates.end());

        UNIT_ASSERT_EQUAL(updatesIt->second.Payload, "test");
    }

    Disconnect(1, 0);

    {
        auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfoUpdate>(edgeSubscriber);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
        const auto &updates = event->Get()->Updates;
        UNIT_ASSERT(updates.size() == 1);

        auto updatesIt = updates.find(publisher);
        UNIT_ASSERT(updatesIt != updates.end());

        UNIT_ASSERT(updatesIt->second.Dropped);
    }

    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvStateStorage::EvReplicaBoardInfo);
    Context->DispatchEvents(options);

    {
        auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfoUpdate>(edgeSubscriber);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
        const auto &updates = event->Get()->Updates;
        UNIT_ASSERT(updates.size() == 1);

        auto updatesIt = updates.find(publisher);
        UNIT_ASSERT(updatesIt != updates.end());

        UNIT_ASSERT_EQUAL(updatesIt->second.Payload, "test");
    }
}

void TBoardSubscriberTestBase::KeepInfoFromOtherRingGroup() {
    const TActorId proxy = MakeStateStorageProxyID();
    const TActorId resolveEdge = Context->AllocateEdgeActor();
    Context->Send(proxy, resolveEdge, new TEvStateStorage::TEvResolveBoard("path"));
    auto resolved = Context->GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(resolveEdge);
    const auto& replicaGroups = resolved->Get()->ReplicaGroups;
    UNIT_ASSERT_VALUES_EQUAL(replicaGroups.size(), 2);

    const TActorId firstReplica = replicaGroups[0].Replicas[0];
    const TActorId secondReplica = replicaGroups[1].Replicas[0];
    auto prevObserverFunc = Context->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvStateStorage::TEvReplicaBoardPublish::EventType
                && ev->Recipient != firstReplica && ev->Recipient != secondReplica) {
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

    size_t publishAcks = 0;
    auto ackObserver = Context->AddObserver<TEvStateStorage::TEvReplicaBoardPublishAck>(
        [&](TEvStateStorage::TEvReplicaBoardPublishAck::TPtr&) { ++publishAcks; });
    const auto edgePublisher = Context->AllocateEdgeActor(0);
    const TActorId publisher = CreatePublisher("path", "test", edgePublisher, 0);
    Context->WaitFor("publication", [&] { return publishAcks == 2; }, TDuration::Seconds(10));

    NActors::TBlockEvents<TEvStateStorage::TEvReplicaBoardInfo> responses(*Context);
    const auto edgeSubscriber = Context->AllocateEdgeActor(1);
    CreateSubscriber("path", edgeSubscriber, 1);
    Context->WaitFor("replica responses", [&] { return responses.size() == 6; }, TDuration::Seconds(10));

    size_t dataResponses = 0;
    for (const auto& response : responses) {
        if (response->Get()->Record.InfoSize()) {
            ++dataResponses;
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(dataResponses, 2);
    responses.Stop().Unblock();
    auto initial = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfo>(edgeSubscriber);
    UNIT_ASSERT_EQUAL(initial->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
    UNIT_ASSERT_VALUES_EQUAL(initial->Get()->InfoEntries.at(publisher).Payload, "test");

    NActors::TBlockEvents<TEvStateStorage::TEvBoardInfoUpdate> updates(*Context);
    Send(firstReplica, TActorId(), new TEvents::TEvPoisonPill(), 0, true);
    Context->SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(updates.empty());

    Send(secondReplica, TActorId(), new TEvents::TEvPoisonPill(), 3, true);
    Context->WaitFor("drop update", [&] { return updates.size() == 1; }, TDuration::Seconds(10));
    const auto& dropUpdates = updates.front()->Get()->Updates;
    UNIT_ASSERT_VALUES_EQUAL(dropUpdates.size(), 1);
    UNIT_ASSERT(dropUpdates.at(publisher).Dropped);
}

void TBoardSubscriberTestBase::IgnoreWriteOnlyRingGroupOnLookup() {
    const TActorId proxy = MakeStateStorageProxyID();
    const TActorId resolveEdge = Context->AllocateEdgeActor();
    Context->Send(proxy, resolveEdge, new TEvStateStorage::TEvResolveBoard("path"));
    auto resolved = Context->GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(resolveEdge);
    const auto& replicaGroups = resolved->Get()->ReplicaGroups;
    UNIT_ASSERT_VALUES_EQUAL(replicaGroups.size(), 2);
    UNIT_ASSERT(!replicaGroups[0].WriteOnly);
    UNIT_ASSERT(replicaGroups[1].WriteOnly);

    const TActorId dataReplica = replicaGroups[0].Replicas[1];
    auto prevObserverFunc = Context->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvStateStorage::TEvReplicaBoardPublish::EventType
                && ev->Recipient != dataReplica) {
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

    size_t publishAcks = 0;
    auto ackObserver = Context->AddObserver<TEvStateStorage::TEvReplicaBoardPublishAck>(
        [&](TEvStateStorage::TEvReplicaBoardPublishAck::TPtr&) { ++publishAcks; });
    const auto edgePublisher = Context->AllocateEdgeActor(0);
    const TActorId publisher = CreatePublisher("path", "test", edgePublisher, 0);
    Context->WaitFor("publication", [&] { return publishAcks == 1; }, TDuration::Seconds(10));

    NActors::TBlockEvents<TEvStateStorage::TEvReplicaBoardInfo> dataResponse(
        *Context, [](const TEvStateStorage::TEvReplicaBoardInfo::TPtr& ev) {
            return !ev->Get()->Record.GetDropped();
        });
    const auto edgeLookup = Context->AllocateEdgeActor(0);
    size_t lookupReplies = 0;
    auto lookupObserver = Context->AddObserver<TEvStateStorage::TEvBoardInfo>(
        [&](TEvStateStorage::TEvBoardInfo::TPtr&) { ++lookupReplies; });
    CreateLookup("path", edgeLookup, EBoardLookupMode::First, 0);
    Context->WaitFor("data response", [&] { return dataResponse.size() == 1; }, TDuration::Seconds(10));
    Context->SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT_VALUES_EQUAL(lookupReplies, 0);

    dataResponse.Stop().Unblock();
    auto result = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfo>(edgeLookup);
    UNIT_ASSERT_EQUAL(result->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->InfoEntries.at(publisher).Payload, "test");
}

void TBoardSubscriberTestBase::ReturnNotAvailableWithoutReadableRingGroup() {
    const auto edgeLookup = Context->AllocateEdgeActor(0);
    CreateLookup("path", edgeLookup, EBoardLookupMode::First, 0);

    auto result = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfo>(edgeLookup);
    UNIT_ASSERT_EQUAL(result->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Path, "path");
}

} // NKikimr
