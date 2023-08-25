#include "defs.h"
#include <ydb/core/testlib/tablet_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/keyvalue/keyvalue_collect_operation.h>
#include <ydb/core/keyvalue/keyvalue_collector.h>
#include <ydb/core/keyvalue/keyvalue_state.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TKeyValueCollectorTest) {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SETUP
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


// Generation 0..9  10..19  20..29  30..
// Ch 2 Group 0     1       0       2
// Ch 3 Group 3     4       5       3
class TContext {
    const ui32 NodeIndex = 0;
    THolder<TTestActorRuntime> Runtime;
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    TActorId CollectorId;
    TActorId TabletActorId;
    TActorId Sender;
public:

    void SetActor(IActor *actor) {
        CollectorId = Runtime->Register(actor, NodeIndex);
    }

    void Setup() {
        Runtime.Reset(new TTestBasicRuntime(1, false));
        //Runtime->SetLogPriority(NKikimrServices::BS_QUEUE, NLog::PRI_CRIT);
        Runtime->Initialize(TAppPrepare().Unwrap());
        TabletInfo.Reset(MakeTabletInfo());

        Sender = Runtime->AllocateEdgeActor(NodeIndex);
        TabletActorId = Runtime->AllocateEdgeActor(NodeIndex);
        for (ui32 groupId = 0; groupId < 6; ++groupId) {
            const TActorId actorId = Runtime->AllocateEdgeActor(NodeIndex);
            const TActorId proxyId = MakeBlobStorageProxyID(groupId);
            Runtime->RegisterService(proxyId, actorId, NodeIndex);
        }
    }

    TIntrusivePtr<TTabletStorageInfo> MakeTabletInfo() {
        TIntrusivePtr<TTabletStorageInfo> x(new TTabletStorageInfo());
        x->TabletID = MakeTabletID(0, 0, 1);
        x->TabletType = TTabletTypes::KeyValue;
        x->Channels.resize(4);
        for (ui64 channel = 0; channel < x->Channels.size(); ++channel) {
            x->Channels[channel].Channel = channel;
            x->Channels[channel].Type = TBlobStorageGroupType(TErasureType::ErasureNone);
            x->Channels[channel].History.resize(4);
            x->Channels[channel].History[0].FromGeneration = 0;
            x->Channels[channel].History[0].GroupID = GetGroupId(channel, 0);
            x->Channels[channel].History[1].FromGeneration = 10;
            x->Channels[channel].History[1].GroupID = GetGroupId(channel, 10);
            x->Channels[channel].History[2].FromGeneration = 20;
            x->Channels[channel].History[2].GroupID = GetGroupId(channel, 20);
            x->Channels[channel].History[3].FromGeneration = 30;
            x->Channels[channel].History[3].GroupID = GetGroupId(channel, 30);
        }
        return x;
    }

    ui32 GetGroupId(ui32 channel, ui32 generation) {
        if (generation < 10) {
            return (channel == 3 ? 3 : 0);
        }
        if (generation < 20) {
            return (channel == 3 ? 4 : 1);
        }
        if (generation < 30) {
            return (channel == 3 ? 5 : 0);
        }
        return (channel == 3 ? 3 : 2);
    }

    TActorId GetProxyActorId(ui32 channel, ui32 generation) {
        ui32 groupId = GetGroupId(channel, generation);
        return MakeBlobStorageProxyID(groupId);
    }

    void Send(IEventBase *ev) {
        Runtime->Send(new IEventHandle(CollectorId, Sender, ev));
    }

    TActorId GetTabletActorId() {
        return TabletActorId;
    }

    TIntrusivePtr<TTabletStorageInfo>& GetTabletInfo() {
        return TabletInfo;
    }

    template <typename TEvent>
    TEvent* GrabEvent(TAutoPtr<IEventHandle>& handle) {
        return Runtime->GrabEdgeEventRethrow<TEvent>(handle);
    }

    void AllowSchedule(TActorId actorId) {
        Runtime->EnableScheduleForActor(actorId);
    }
};


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TEST CASES
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST(TestKeyValueCollectorEmpty) {
    TContext context;
    context.Setup();

    TVector<TLogoBlobID> keep;
    TVector<TLogoBlobID> doNotKeep;
    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), std::move(doNotKeep), {}));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200, true));

    //TLogoBlobID logoblobid(0x10010000001000Bull, 5, 58949, 1, 1209816, 10);

    for (ui32 idx = 0; idx < 6; ++idx) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);

        context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel));
    }

    TAutoPtr<IEventHandle> handle;
    auto eraseCollect = context.GrabEvent<TEvKeyValue::TEvCompleteGC>(handle);
    UNIT_ASSERT(eraseCollect);
}

Y_UNIT_TEST(TestKeyValueCollectorSingle) {
    TContext context;
    context.Setup();

    TVector<TLogoBlobID> keep;
    keep.emplace_back(0x10010000001000Bull, 5, 58949, NKeyValue::BLOB_CHANNEL, 1209816, 10);
    TVector<TLogoBlobID> doNotKeep;
    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), std::move(doNotKeep), {}));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200, true));

    ui32 erased = 0;
    for (ui32 idx = 0; idx < 6; ++idx) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        if (handle->Recipient == context.GetProxyActorId(NKeyValue::BLOB_CHANNEL, 5)) {
            UNIT_ASSERT(collect->Keep.Get());
            UNIT_ASSERT(collect->Keep->size() == 1);
            ui32 generation = (*collect->Keep)[0].Generation();
            UNIT_ASSERT(handle->Recipient == context.GetProxyActorId(collect->Channel, generation));
            ++erased;
        } else {
            UNIT_ASSERT(!collect->Keep.Get());
        }

        context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel));
    }
    UNIT_ASSERT(erased == 1);

    TAutoPtr<IEventHandle> handle;
    auto eraseCollect = context.GrabEvent<TEvKeyValue::TEvCompleteGC>(handle);
    UNIT_ASSERT(eraseCollect);
}

Y_UNIT_TEST(TestKeyValueCollectorSingleWithOneError) {
    TContext context;
    context.Setup();

    TVector<TLogoBlobID> keep;
    keep.emplace_back(0x10010000001000Bull, 5, 58949, NKeyValue::BLOB_CHANNEL, 1209816, 10);
    TVector<TLogoBlobID> doNotKeep;
    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), std::move(doNotKeep), {}));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200, true));

    ui32 erased = 0;
    for (ui32 idx = 0; idx < 6; ++idx) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        if (handle->Recipient == context.GetProxyActorId(NKeyValue::BLOB_CHANNEL, 5)) {
            UNIT_ASSERT(collect->Keep.Get());
            UNIT_ASSERT(collect->Keep->size() == 1);
            auto keep = *collect->Keep;
            ui32 generation = (*collect->Keep)[0].Generation();
            UNIT_ASSERT(handle->Recipient == context.GetProxyActorId(collect->Channel, generation));
            context.AllowSchedule(handle->Sender);

            context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::ERROR, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel));
            collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
            UNIT_ASSERT(collect->Keep.Get());
            UNIT_ASSERT(collect->Keep->size() == 1);
            UNIT_ASSERT(keep == *collect->Keep);
            generation = (*collect->Keep)[0].Generation();
            UNIT_ASSERT(handle->Recipient == context.GetProxyActorId(collect->Channel, generation));

            ++erased;
        } else {
            UNIT_ASSERT(!collect->Keep.Get());
        }

        context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel));
    }
    UNIT_ASSERT(erased == 1);

    TAutoPtr<IEventHandle> handle;
    auto eraseCollect = context.GrabEvent<TEvKeyValue::TEvCompleteGC>(handle);
    UNIT_ASSERT(eraseCollect);
}

Y_UNIT_TEST(TestKeyValueCollectorMultiple) {
    TContext context;
    context.Setup();

    TVector<TLogoBlobID> keep;
    TVector<TLogoBlobID> doNotKeep;
    doNotKeep.emplace_back(0x10010000001000Bull, 5, 58949, NKeyValue::BLOB_CHANNEL, 1209816, 10);
    doNotKeep.emplace_back(0x10010000001000Bull, 15, 58949, NKeyValue::BLOB_CHANNEL, 1209816, 10);
    doNotKeep.emplace_back(0x10010000001000Bull, 25, 58949, NKeyValue::BLOB_CHANNEL, 1209816, 10);
    doNotKeep.emplace_back(0x10010000001000Bull, 35, 58949, NKeyValue::BLOB_CHANNEL, 1209816, 10);

    doNotKeep.emplace_back(0x10010000001000Bull, 5, 58949, NKeyValue::BLOB_CHANNEL + 1, 1209816, 10);
    doNotKeep.emplace_back(0x10010000001000Bull, 15, 58949, NKeyValue::BLOB_CHANNEL + 1, 1209816, 10);
    doNotKeep.emplace_back(0x10010000001000Bull, 25, 58949, NKeyValue::BLOB_CHANNEL + 1, 1209816, 10);
    doNotKeep.emplace_back(0x10010000001000Bull, 35, 58949, NKeyValue::BLOB_CHANNEL + 1, 1209816, 10);

    TSet<TLogoBlobID> ids;
    for (ui32 i = 0; i < doNotKeep.size(); ++i) {
        ids.insert(doNotKeep[i]);
    }

    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), std::move(doNotKeep), {}));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200, true));

    ui32 erased = 0;
    for (ui32 idx = 0; idx < 6; ++idx) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        if (collect->DoNotKeep && collect->DoNotKeep->size()) {
            context.AllowSchedule(handle->Sender);
            context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::ERROR, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel));
            collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        }
        bool isPresent = false;
        for (auto it = ids.begin(); it != ids.end(); ++it) {
            if (handle->Recipient == context.GetProxyActorId(it->Channel(), it->Generation())) {
                UNIT_ASSERT(collect->DoNotKeep.Get());
                for (ui32 doNotKeepIdx = 0; doNotKeepIdx < collect->DoNotKeep->size(); ++doNotKeepIdx) {
                    if ((*collect->DoNotKeep)[doNotKeepIdx] == *it) {
                        ++erased;
                        isPresent = true;
                    }
                }
            }
        }
        if (!isPresent) {
            UNIT_ASSERT(!collect->DoNotKeep.Get());
        }

        context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel));
    }
    UNIT_ASSERT(erased == 8);

    TAutoPtr<IEventHandle> handle;
    auto eraseCollect = context.GrabEvent<TEvKeyValue::TEvCompleteGC>(handle);
    UNIT_ASSERT(eraseCollect);
}


Y_UNIT_TEST(TestKeyValueCollectorMany) {
    TContext context;
    context.Setup();

    TVector<TLogoBlobID> keep;
    TVector<TLogoBlobID> doNotKeep;
    doNotKeep.reserve(MaxCollectGarbageFlagsPerMessage * 2);
    doNotKeep.reserve(MaxCollectGarbageFlagsPerMessage * 2);
    for (ui32 idx = 0; idx < MaxCollectGarbageFlagsPerMessage * 2; ++idx) {
        doNotKeep.emplace_back(0x10010000001000Bull, idx, 58949, NKeyValue::BLOB_CHANNEL, 1209816, 10);
        keep.emplace_back(0x10010000001000Bull, idx, 58949, NKeyValue::BLOB_CHANNEL, 1209816, 10);
    }

    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), std::move(doNotKeep), {}));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200, true));

    auto handleGC = [&](ui32 keepSize, ui32 doNotKeepSize) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        UNIT_ASSERT_VALUES_EQUAL((collect->Keep ? collect->Keep->size() : 0), keepSize);
        UNIT_ASSERT_VALUES_EQUAL((collect->DoNotKeep ? collect->DoNotKeep->size() : 0), doNotKeepSize);
        context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel));
    };

    handleGC(20, 20); // group 0
    handleGC(10, 10); // group 1
    handleGC(0, 10'000); // group 2 DoNotKeep 30..10029
    handleGC(30, 9'970); // group 2 DoNotKeep 10030..19999 Keep 30.59
    handleGC(10'000, 0); // group 2  Keep 60..10059
    handleGC(9'940, 0); // group 2  Keep 10060..20000
    handleGC(0, 0); // group 3
    handleGC(0, 0); // group 4
    handleGC(0, 0); // group 5

    TAutoPtr<IEventHandle> handle;
    auto eraseCollect = context.GrabEvent<TEvKeyValue::TEvCompleteGC>(handle);
    UNIT_ASSERT(eraseCollect);
}

} // TKeyValueCollectorTest
} // NKikimr
