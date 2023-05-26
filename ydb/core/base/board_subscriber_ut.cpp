#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/statestorage_impl.h>

#include <ydb/core/base/pathid.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>

#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/interconnect/interconnect_impl.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/basics/runtime.h>

#include <util/generic/vector.h>
#include <util/generic/xrange.h>

namespace NKikimr {

class TBoardSubscriberTest: public NUnitTest::TTestBase {

    TActorId CreateSubscriber(const TString& path, const TActorId& owner, ui32 nodeIdx) {
        const TActorId subscriber = Context->Register(
            CreateBoardLookupActor(path, owner, 0, EBoardLookupMode::Subscription), nodeIdx
        );
        Context->EnableScheduleForActor(subscriber);
        return subscriber;
    }

    TActorId CreatePublisher(
            const TString& path, const TString& payload, const TActorId& owner, ui32 nodeIdx) {
        const TActorId publisher = Context->Register(
            CreateBoardPublishActor(path, payload, owner, 0, 0, true), nodeIdx
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

public:
    void SetUp() override {
        Context = MakeHolder<TTestBasicRuntime>(2);

        for (ui32 i : xrange(Context->GetNodeCount())) {
            SetupStateStorage(*Context, i, 0);
        }

        Context->Initialize(TAppPrepare().Unwrap());
    }

    void TearDown() override {
        Context.Reset();
    }

    UNIT_TEST_SUITE(TBoardSubscriberTest);
    UNIT_TEST(SimpleSubscriber);
    UNIT_TEST(ManySubscribersManyPublisher);
    UNIT_TEST(DisconnectReplica);
    UNIT_TEST_SUITE_END();

    void SimpleSubscriber();
    void ManySubscribersManyPublisher();
    void DisconnectReplica();

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
        const auto &update = event->Get()->Update;

        UNIT_ASSERT(!update.Dropped);
        UNIT_ASSERT_EQUAL(update.Owner, publisher);
        UNIT_ASSERT_EQUAL(update.Payload, "test");
    }

    KillPublisher(edgePublisher, publisher, 1);

    {
        auto event = Context->GrabEdgeEventIf<TEvStateStorage::TEvBoardInfoUpdate>(
            {edgeSubscriber}, [edgeSubscriber](const auto& ev){
            const auto &update = ev->Get()->Update;
            if (ev->Recipient == edgeSubscriber && update.Dropped) {
                return true;
            }
            return false;
        });
        UNIT_ASSERT(event);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
        UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
        const auto &update = event->Get()->Update;

        UNIT_ASSERT(update.Dropped);
        UNIT_ASSERT_EQUAL(update.Owner, publisher);
    }
}

void TBoardSubscriberTest::ManySubscribersManyPublisher() {
    size_t subscribersCount = 10;
    size_t publishersCount = 10;

    THashSet<TActorId> subscribers;

    for (size_t i = 0; i < subscribersCount; i++) {
        const auto edgeSubscriber = Context->AllocateEdgeActor(i % 2);
        CreateSubscriber("path", edgeSubscriber, i % 2);
        subscribers.insert(edgeSubscriber);
        {
            auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfo>(edgeSubscriber);
            UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
            UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
            UNIT_ASSERT(event->Get()->InfoEntries.empty());
        }
    }

    for (size_t i = 0; i < publishersCount; i++) {
        const auto edgePublisher = Context->AllocateEdgeActor(i % 2);
        const auto publisher = CreatePublisher("path", ToString(i), edgePublisher, i % 2);
        for (const auto& subscriber : subscribers) {
            auto event = Context->GrabEdgeEventIf<TEvStateStorage::TEvBoardInfoUpdate>(
                {subscriber}, [publisher](const auto& ev) mutable {
                    if (ev->Get()->Update.Owner == publisher) {
                        return true;
                    }
                    return false;
                });
            UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
            UNIT_ASSERT_EQUAL(event->Get()->Update.Payload, ToString(i));
        }
    }
}

void TBoardSubscriberTest::DisconnectReplica() {

    std::vector<TActorId> replicas;

    {
        const auto edgeSubscriber = Context->AllocateEdgeActor(1);
        CreateSubscriber("path", edgeSubscriber, 1);

        {
            auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfo>(edgeSubscriber);
            UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
            UNIT_ASSERT_EQUAL(event->Get()->Path, "path");
            UNIT_ASSERT(event->Get()->InfoEntries.empty());
        }

        Disconnect(1, 0);

        auto event = Context->GrabEdgeEvent<TEvStateStorage::TEvBoardInfoUpdate>(edgeSubscriber);
        UNIT_ASSERT_EQUAL(event->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable);
    }
}

} // NKikimr
