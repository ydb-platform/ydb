#include "defs.h"
#include <ydb/core/testlib/tablet_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/keyvalue/keyvalue_collect_operation.h>
#include <ydb/core/keyvalue/keyvalue_collector.h>
#include <ydb/core/keyvalue/keyvalue_flat_impl.h>
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
        x->TabletID = MakeTabletID(false, 1);
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

    void Send(IEventBase *ev, ui64 cookie = 0) {
        Runtime->Send(new IEventHandle(CollectorId, Sender, ev, 0, cookie));
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

    // nullptr when nothing arrives: the runtime throws once the queue stays empty for the dispatch timeout
    template <typename TEvent>
    TEvent* GrabEventOrNull(TAutoPtr<IEventHandle>& handle, TDuration simTimeout) {
        const TDuration savedTimeout = Runtime->SetDispatchTimeout(TDuration::Seconds(1));
        TEvent* event = nullptr;
        try {
            event = Runtime->GrabEdgeEvent<TEvent>(handle, simTimeout);
        } catch (const NActors::TEmptyEventQueueException&) {
        }
        Runtime->SetDispatchTimeout(savedTimeout);
        return event;
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
    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), std::move(doNotKeep), {}, true));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200));

    //TLogoBlobID logoblobid(0x10010000001000Bull, 5, 58949, 1, 1209816, 10);

    for (ui32 idx = 0; idx < 2; ++idx) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);

        context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
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
    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), std::move(doNotKeep), {}, true));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200));

    ui32 erased = 0;
    for (ui32 idx = 0; idx < 3; ++idx) {
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
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
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
    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, TVector<TLogoBlobID>(keep), {}, {}, true));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200));

    ui32 erased = 0;
    bool flag = true;
    for (ui32 idx = 0; idx < 4; ++idx) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        if (handle->Recipient == context.GetProxyActorId(NKeyValue::BLOB_CHANNEL, 5)) {
            UNIT_ASSERT(collect->Keep);
            UNIT_ASSERT(*collect->Keep == keep);
            if (flag) {
                context.AllowSchedule(handle->Sender);
                context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::ERROR, collect->TabletId,
                        collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
            } else {
                context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, collect->TabletId,
                        collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
                ++erased;
            }
            flag = false;
        } else {
            UNIT_ASSERT(!collect->Keep);
            context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
        }
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

    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), std::move(doNotKeep), {}, true));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200));

    ui32 erased = 0;
    for (ui32 idx = 0; idx < 6; ++idx) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        if (collect->DoNotKeep && collect->DoNotKeep->size()) {
            context.AllowSchedule(handle->Sender);
            context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::ERROR, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
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
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
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

    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), std::move(doNotKeep), {}, true));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200));

    using TFlagCounts = std::pair<size_t, size_t>;
    TVector<TFlagCounts> smallMessages;
    TVector<TFlagCounts> chunkedMessages;
    const TActorId chunkedProxy = context.GetProxyActorId(NKeyValue::BLOB_CHANNEL, 30);
    for (ui32 idx = 0; idx < 7; ++idx) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        const TFlagCounts counts(collect->Keep ? collect->Keep->size() : 0, collect->DoNotKeep ? collect->DoNotKeep->size() : 0);
        (handle->Recipient == chunkedProxy ? chunkedMessages : smallMessages).push_back(counts);
        context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
    }

    // group 2 holds 19970 Keep and 19970 DoNotKeep flags, DoNotKeep is packed into the tail of the last Keep chunk
    const size_t chunk = NKeyValue::CollectorMaxFlagsPerMessage;
    const size_t bigGroupFlags = MaxCollectGarbageFlagsPerMessage * 2 - 30;
    const size_t keepTail = bigGroupFlags - chunk;
    Sort(smallMessages);
    UNIT_ASSERT((smallMessages == TVector<TFlagCounts>{{0, 0}, {10, 10}, {20, 20}})); // groups 3, 1, 0
    UNIT_ASSERT((chunkedMessages == TVector<TFlagCounts>{{chunk, 0}, {keepTail, chunk - keepTail},
        {0, chunk}, {0, bigGroupFlags - (chunk - keepTail) - chunk}})); // group 2

    TAutoPtr<IEventHandle> handle;
    auto eraseCollect = context.GrabEvent<TEvKeyValue::TEvCompleteGC>(handle);
    UNIT_ASSERT(eraseCollect);
}

void TestChunkedCollect(ui32 failedMessageIndex) {
    TContext context;
    context.Setup();

    const ui32 dataGeneration = 35;
    const ui32 keepCount = NKeyValue::CollectorMaxFlagsPerMessage * 2 + 500;
    const ui32 doNotKeepCount = NKeyValue::CollectorMaxFlagsPerMessage / 2;
    TVector<TLogoBlobID> keep;
    TVector<TLogoBlobID> doNotKeep;
    for (ui32 idx = 0; idx < keepCount; ++idx) {
        keep.emplace_back(0x10010000001000Bull, dataGeneration, idx + 1, NKeyValue::BLOB_CHANNEL, 100, 0);
    }
    for (ui32 idx = 0; idx < doNotKeepCount; ++idx) {
        doNotKeep.emplace_back(0x10010000001000Bull, dataGeneration - 1, idx + 1, NKeyValue::BLOB_CHANNEL, 100, 0);
    }
    const TSet<TLogoBlobID> expectedKeep(keep.begin(), keep.end());
    const TSet<TLogoBlobID> expectedDoNotKeep(doNotKeep.begin(), doNotKeep.end());

    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), std::move(doNotKeep), {}, true));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200));

    const TActorId dataProxy = context.GetProxyActorId(NKeyValue::BLOB_CHANNEL, dataGeneration);
    TSet<TLogoBlobID> deliveredKeep;
    TSet<TLogoBlobID> deliveredDoNotKeep;
    std::optional<std::pair<TVector<TLogoBlobID>, TVector<TLogoBlobID>>> failedChunk;
    ui32 dataMessages = 0;
    bool isDataGroupDone = false;
    bool isOtherGroupDone = false;

    while (!isDataGroupDone || !isOtherGroupDone) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        const TVector<TLogoBlobID> keepChunk = collect->Keep ? *collect->Keep : TVector<TLogoBlobID>();
        const TVector<TLogoBlobID> doNotKeepChunk = collect->DoNotKeep ? *collect->DoNotKeep : TVector<TLogoBlobID>();
        UNIT_ASSERT(keepChunk.size() + doNotKeepChunk.size() <= NKeyValue::CollectorMaxFlagsPerMessage);
        UNIT_ASSERT_VALUES_EQUAL(collect->RecordGeneration, 200);
        UNIT_ASSERT_VALUES_EQUAL(collect->PerGenerationCounter, 200);
        UNIT_ASSERT_VALUES_EQUAL(collect->CollectGeneration, 100);
        UNIT_ASSERT_VALUES_EQUAL(collect->CollectStep, 100);

        auto reply = [&](NKikimrProto::EReplyStatus status) {
            context.Send(new TEvBlobStorage::TEvCollectGarbageResult(status, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
        };

        if (handle->Recipient != dataProxy) {
            UNIT_ASSERT(!isOtherGroupDone);
            UNIT_ASSERT(keepChunk.empty() && doNotKeepChunk.empty());
            UNIT_ASSERT(collect->Collect);
            isOtherGroupDone = true;
            reply(NKikimrProto::OK);
            continue;
        }

        UNIT_ASSERT(!isDataGroupDone);
        ++dataMessages;
        if (failedChunk) {
            UNIT_ASSERT(failedChunk->first == keepChunk);
            UNIT_ASSERT(failedChunk->second == doNotKeepChunk);
            failedChunk.reset();
        } else if (dataMessages == failedMessageIndex) {
            failedChunk.emplace(keepChunk, doNotKeepChunk);
            context.AllowSchedule(handle->Sender);
            reply(NKikimrProto::ERROR);
            continue;
        }

        for (const TLogoBlobID& id : keepChunk) {
            UNIT_ASSERT(deliveredKeep.insert(id).second);
        }
        for (const TLogoBlobID& id : doNotKeepChunk) {
            UNIT_ASSERT(deliveredDoNotKeep.insert(id).second);
        }
        const bool isEverythingDelivered = deliveredKeep.size() == expectedKeep.size() &&
            deliveredDoNotKeep.size() == expectedDoNotKeep.size();
        UNIT_ASSERT_VALUES_EQUAL(collect->Collect, isEverythingDelivered);
        isDataGroupDone = collect->Collect;
        reply(NKikimrProto::OK);
    }

    UNIT_ASSERT(deliveredKeep == expectedKeep);
    UNIT_ASSERT(deliveredDoNotKeep == expectedDoNotKeep);
    UNIT_ASSERT_VALUES_EQUAL(dataMessages, failedMessageIndex ? 4 : 3);

    TAutoPtr<IEventHandle> handle;
    auto eraseCollect = context.GrabEvent<TEvKeyValue::TEvCompleteGC>(handle);
    UNIT_ASSERT(eraseCollect);
}

Y_UNIT_TEST(TestKeyValueCollectorChunks) {
    TestChunkedCollect(0);
}

Y_UNIT_TEST(TestKeyValueCollectorChunksWithOneError) {
    TestChunkedCollect(2);
}

Y_UNIT_TEST(TestKeyValueCollectorChunksWithErrorOnLastChunk) {
    TestChunkedCollect(3);
}

Y_UNIT_TEST(TestKeyValueCollectorExactlyOneChunk) {
    TContext context;
    context.Setup();

    const ui32 dataGeneration = 35;
    TVector<TLogoBlobID> keep;
    for (ui32 idx = 0; idx < NKeyValue::CollectorMaxFlagsPerMessage; ++idx) {
        keep.emplace_back(0x10010000001000Bull, dataGeneration, idx + 1, NKeyValue::BLOB_CHANNEL, 100, 0);
    }
    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), {}, {}, true));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200));

    const TActorId dataProxy = context.GetProxyActorId(NKeyValue::BLOB_CHANNEL, dataGeneration);
    ui32 dataMessages = 0;
    for (ui32 idx = 0; idx < 2; ++idx) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        UNIT_ASSERT(collect->Collect);
        UNIT_ASSERT(!collect->DoNotKeep);
        if (handle->Recipient == dataProxy) {
            UNIT_ASSERT(collect->Keep);
            UNIT_ASSERT_VALUES_EQUAL(collect->Keep->size(), NKeyValue::CollectorMaxFlagsPerMessage);
            ++dataMessages;
        } else {
            UNIT_ASSERT(!collect->Keep);
        }
        context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
    }
    UNIT_ASSERT_VALUES_EQUAL(dataMessages, 1);

    TAutoPtr<IEventHandle> handle;
    auto eraseCollect = context.GrabEvent<TEvKeyValue::TEvCompleteGC>(handle);
    UNIT_ASSERT(eraseCollect);
}

Y_UNIT_TEST(TestKeyValueCollectorDoNotKeepOnly) {
    TContext context;
    context.Setup();

    const ui32 dataGeneration = 35;
    const ui32 doNotKeepCount = NKeyValue::CollectorMaxFlagsPerMessage + NKeyValue::CollectorMaxFlagsPerMessage / 2;
    TVector<TLogoBlobID> doNotKeep;
    for (ui32 idx = 0; idx < doNotKeepCount; ++idx) {
        doNotKeep.emplace_back(0x10010000001000Bull, dataGeneration, idx + 1, NKeyValue::BLOB_CHANNEL, 100, 0);
    }
    const TSet<TLogoBlobID> expectedDoNotKeep(doNotKeep.begin(), doNotKeep.end());
    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, {}, std::move(doNotKeep), {}, false));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200));

    const TActorId dataProxy = context.GetProxyActorId(NKeyValue::BLOB_CHANNEL, dataGeneration);
    TSet<TLogoBlobID> deliveredDoNotKeep;
    while (deliveredDoNotKeep.size() < expectedDoNotKeep.size()) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        UNIT_ASSERT(handle->Recipient == dataProxy);
        UNIT_ASSERT(!collect->Collect);
        UNIT_ASSERT(!collect->Keep);
        UNIT_ASSERT(collect->DoNotKeep);
        UNIT_ASSERT(collect->DoNotKeep->size() <= NKeyValue::CollectorMaxFlagsPerMessage);
        for (const TLogoBlobID& id : *collect->DoNotKeep) {
            UNIT_ASSERT(deliveredDoNotKeep.insert(id).second);
        }
        context.Send(new TEvBlobStorage::TEvCollectGarbageResult(NKikimrProto::OK, collect->TabletId,
                    collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
    }
    UNIT_ASSERT(deliveredDoNotKeep == expectedDoNotKeep);

    TAutoPtr<IEventHandle> handle;
    auto eraseCollect = context.GrabEvent<TEvKeyValue::TEvCompleteGC>(handle);
    UNIT_ASSERT(eraseCollect);
}

TVector<TLogoBlobID> MakeBlobIds(ui32 generation, ui32 channel, ui32 count) {
    TVector<TLogoBlobID> ids;
    ids.reserve(count);
    for (ui32 idx = 0; idx < count; ++idx) {
        ids.emplace_back(0x10010000001000Bull, generation, idx + 1, channel, 100, 0);
    }
    return ids;
}

void ReplyToCollect(TContext& context, const TEvBlobStorage::TEvCollectGarbage* collect, const TAutoPtr<IEventHandle>& handle,
        NKikimrProto::EReplyStatus status) {
    context.Send(new TEvBlobStorage::TEvCollectGarbageResult(status, collect->TabletId,
                collect->RecordGeneration, collect->PerGenerationCounter, collect->Channel), handle->Cookie);
}

Y_UNIT_TEST(TestKeyValueCollectorWaitsForChunkAck) {
    TContext context;
    context.Setup();

    const ui32 dataGeneration = 35;
    const ui32 slowChannel = NKeyValue::BLOB_CHANNEL;
    const ui32 fastChannel = NKeyValue::BLOB_CHANNEL + 1;
    const ui32 slowKeepCount = NKeyValue::CollectorMaxFlagsPerMessage * 2 + 1;
    TVector<TLogoBlobID> keep = MakeBlobIds(dataGeneration, slowChannel, slowKeepCount);
    const TVector<TLogoBlobID> fastKeep = MakeBlobIds(dataGeneration, fastChannel, 3);
    keep.insert(keep.end(), fastKeep.begin(), fastKeep.end());

    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), {}, {}, true));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200));

    const TActorId slowProxy = context.GetProxyActorId(slowChannel, dataGeneration);
    const TActorId fastProxy = context.GetProxyActorId(fastChannel, dataGeneration);
    const TDuration quiet = TDuration::Seconds(30);

    // one request per group/channel: the slow group gets its first chunk, the fast one its only chunk with the barrier
    TAutoPtr<IEventHandle> heldHandle;
    const TEvBlobStorage::TEvCollectGarbage* heldCollect = nullptr;
    for (ui32 idx = 0; idx < 2; ++idx) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        if (handle->Recipient == slowProxy) {
            UNIT_ASSERT(!collect->Collect);
            UNIT_ASSERT_VALUES_EQUAL(collect->Keep->size(), NKeyValue::CollectorMaxFlagsPerMessage);
            heldHandle = handle;
            heldCollect = collect;
        } else {
            UNIT_ASSERT(handle->Recipient == fastProxy);
            UNIT_ASSERT(collect->Collect);
            UNIT_ASSERT_VALUES_EQUAL(collect->Keep->size(), fastKeep.size());
            ReplyToCollect(context, collect, handle, NKikimrProto::OK);
        }
    }
    UNIT_ASSERT(heldCollect);

    // the fast group finished, nothing else leaves the collector while the first slow chunk is unacked
    {
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!context.GrabEventOrNull<TEvBlobStorage::TEvCollectGarbage>(handle, quiet));
        UNIT_ASSERT(!context.GrabEventOrNull<TEvKeyValue::TEvCompleteGC>(handle, quiet));
    }

    ReplyToCollect(context, heldCollect, heldHandle, NKikimrProto::OK);
    size_t keepSeen = heldCollect->Keep->size();
    for (bool done = false; !done;) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        UNIT_ASSERT(handle->Recipient == slowProxy);
        keepSeen += collect->Keep->size();
        done = collect->Collect;
        UNIT_ASSERT_VALUES_EQUAL(done, keepSeen == slowKeepCount);
        ReplyToCollect(context, collect, handle, NKikimrProto::OK);
    }

    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(context.GrabEvent<TEvKeyValue::TEvCompleteGC>(handle));
}

Y_UNIT_TEST(TestKeyValueCollectorRetryBudgetIsPerChunk) {
    TContext context;
    context.Setup();

    const ui32 dataGeneration = 35;
    const ui32 keepCount = NKeyValue::CollectorMaxFlagsPerMessage * 2 + 1;
    TVector<TLogoBlobID> keep = MakeBlobIds(dataGeneration, NKeyValue::BLOB_CHANNEL, keepCount);
    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), {}, {}, true));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200));

    const TActorId dataProxy = context.GetProxyActorId(NKeyValue::BLOB_CHANNEL, dataGeneration);
    // every chunk fails just short of the limit; a shared budget would have poisoned the tablet on the second chunk
    const ui32 failuresPerChunk = NKeyValue::CollectorMaxErrors - 1;
    ui32 chunksAcked = 0;
    ui32 failuresLeft = failuresPerChunk;
    std::optional<TVector<TLogoBlobID>> lastFailedChunk;
    for (bool done = false; !done;) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        if (handle->Recipient != dataProxy) {
            UNIT_ASSERT(!collect->Keep);
            ReplyToCollect(context, collect, handle, NKikimrProto::OK);
            continue;
        }
        if (lastFailedChunk) {
            UNIT_ASSERT(*lastFailedChunk == *collect->Keep);
        }
        if (failuresLeft) {
            --failuresLeft;
            lastFailedChunk = *collect->Keep;
            context.AllowSchedule(handle->Sender);
            ReplyToCollect(context, collect, handle, NKikimrProto::ERROR);
            continue;
        }
        ++chunksAcked;
        failuresLeft = failuresPerChunk;
        lastFailedChunk.reset();
        done = collect->Collect;
        ReplyToCollect(context, collect, handle, NKikimrProto::OK);
    }
    UNIT_ASSERT_VALUES_EQUAL(chunksAcked, 3);

    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(context.GrabEvent<TEvKeyValue::TEvCompleteGC>(handle));
}

Y_UNIT_TEST(TestKeyValueCollectorExhaustedChunkPoisonsTablet) {
    TContext context;
    context.Setup();

    const ui32 dataGeneration = 35;
    const ui32 keepCount = NKeyValue::CollectorMaxFlagsPerMessage + 1;
    TVector<TLogoBlobID> keep = MakeBlobIds(dataGeneration, NKeyValue::BLOB_CHANNEL, keepCount);
    TIntrusivePtr<NKeyValue::TCollectOperation> operation(new NKeyValue::TCollectOperation(100, 100, std::move(keep), {}, {}, true));
    context.SetActor(CreateKeyValueCollector(
                context.GetTabletActorId(), operation, context.GetTabletInfo().Get(), 200, 200));

    const TActorId dataProxy = context.GetProxyActorId(NKeyValue::BLOB_CHANNEL, dataGeneration);
    ui32 failures = 0;
    while (failures < NKeyValue::CollectorMaxErrors) {
        TAutoPtr<IEventHandle> handle;
        auto collect = context.GrabEvent<TEvBlobStorage::TEvCollectGarbage>(handle);
        UNIT_ASSERT(collect);
        if (handle->Recipient != dataProxy) {
            ReplyToCollect(context, collect, handle, NKikimrProto::OK);
            continue;
        }
        UNIT_ASSERT(!collect->Collect);
        ++failures;
        context.AllowSchedule(handle->Sender);
        ReplyToCollect(context, collect, handle, NKikimrProto::ERROR);
    }

    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(context.GrabEvent<TEvents::TEvPoisonPill>(handle));
    UNIT_ASSERT(handle->Recipient == context.GetTabletActorId());
    UNIT_ASSERT(!context.GrabEventOrNull<TEvKeyValue::TEvCompleteGC>(handle, TDuration::Seconds(30)));
    UNIT_ASSERT(!context.GrabEventOrNull<TEvBlobStorage::TEvCollectGarbage>(handle, TDuration::Seconds(30)));
}

} // TKeyValueCollectorTest
} // NKikimr
