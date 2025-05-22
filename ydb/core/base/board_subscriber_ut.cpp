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

#include <util/generic/vector.h>
#include <util/generic/xrange.h>

namespace NKikimr {

class TBoardSubscriberTest: public NUnitTest::TTestBase {

    TActorId CreateSubscriber(const TString& path, const TActorId& owner, ui32 nodeIdx) {
        const TActorId subscriber = Context->Register(
            CreateBoardLookupActor(path, owner, EBoardLookupMode::Subscription), nodeIdx
        );
        Context->EnableScheduleForActor(subscriber);
        return subscriber;
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

        auto allReplicas = ev->Get()->Replicas;
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

    UNIT_TEST_SUITE(TBoardSubscriberTest);
    UNIT_TEST(SimpleSubscriber);
    UNIT_TEST(ManySubscribersManyPublisher);
    UNIT_TEST(NotAvailableByShutdown);
    UNIT_TEST(ReconnectReplica);
    UNIT_TEST(DropByDisconnect);
    UNIT_TEST_SUITE_END();

    void SimpleSubscriber();
    void ManySubscribersManyPublisher();
    void NotAvailableByShutdown();
    void ReconnectReplica();
    void DropByDisconnect();

private:
    THolder<TTestBasicRuntime> Context;
};

UNIT_TEST_SUITE_REGISTRATION(TBoardSubscriberTest);

void TBoardSubscriberTest::SimpleSubscriber() {
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
            {edgeSubscriber}, [edgeSubscriber, publisher](const auto& ev){
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

void TBoardSubscriberTest::ManySubscribersManyPublisher() {
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
                {subscriber}, [publisher](const auto& ev) mutable {
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

void TBoardSubscriberTest::NotAvailableByShutdown() {

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

void TBoardSubscriberTest::ReconnectReplica() {

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

void TBoardSubscriberTest::DropByDisconnect() {
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

} // NKikimr
