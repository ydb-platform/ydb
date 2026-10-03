#include <ydb/core/persqueue/dread_cache_service/caching_service.h>
#include <ydb/core/persqueue/pqtablet/readproxy/readproxy.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/public/lib/base/msgbus_status.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPQCachingProxyTest) {
struct TTestSetup {
    TTestContext Context;
    TActorId ProxyId;
    TTestSetup() {
        Context.Prepare();
        Context.Runtime->SetLogPriority(NKikimrServices::PQ_READ_PROXY, NLog::PRI_DEBUG);
        ProxyId = Context.Runtime->Register(CreatePQDReadCacheService(new NMonitoring::TDynamicCounters()));
        Context.Runtime->AllocateEdgeActor();
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back(TEvents::TEvBootstrap::EventType, 1);
        Context.Runtime->DispatchEvents(opts);
    }
    auto* GetRuntime() {
        return Context.Runtime.Get();
    }
    THolder<TEvPQ::TEvGetFullDirectReadData> SendRequest(TEvPQ::TEvGetFullDirectReadData* request, bool status = true) {
        GetRuntime()->Send(ProxyId, Context.Edge, request);
        auto resp = GetRuntime()->GrabEdgeEvent<TEvPQ::TEvGetFullDirectReadData>();
        UNIT_ASSERT(resp);
        UNIT_ASSERT(resp->Error != status);
        return resp;
    }
};

Y_UNIT_TEST(DirectReadLastOffsetWaitsForBlobTail) {
    // Offsets 13 and 14 are complete. Offset 15 is only the first part.
    // DirectRead must not drop it; the missing part is requested by a follow-up.
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    runtime->SetScheduledLimit(100000);

    NKikimrClient::TPersQueueRequest request;
    auto* read = request.MutablePartitionRequest()->MutableCmdRead();
    read->SetClientId("user");
    read->SetSessionId("session1");
    read->SetOffset(13);
    read->SetPartNo(0);
    read->SetDirectReadId(1);
    read->SetReadToBlobEnd(true);

    const auto tablet = runtime->AllocateEdgeActor();
    auto proxy = runtime->Register(CreateReadProxy(
            setup.Context.Edge, 1, tablet, 1, TDirectReadKey{"session1", 1, 1}, request, TActorId{}));
    {
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back(TEvents::TEvBootstrap::EventType, 1);
        runtime->DispatchEvents(opts);
    }

    NKikimrClient::TCmdReadResult rows;
    rows.SetRealReadOffset(13);
    rows.SetLastOffset(15);
    rows.SetEndOffset(24);
    auto add = [&](ui64 offset, ui32 partNo, ui32 totalParts) {
        auto* row = rows.AddResult();
        row->SetOffset(offset);
        row->SetData("x");
        row->SetPartNo(partNo);
        if (totalParts) {
            row->SetTotalParts(totalParts);
        }
    };
    add(13, 0, 1);
    add(14, 0, 1);
    add(15, 0, 2);
    // Glued body is "x" + "y". TotalSize is taken from the first part.
    rows.MutableResult(rows.ResultSize() - 1)->SetTotalSize(2);

    auto response = MakeHolder<TEvPersQueue::TEvResponse>();
    response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    response->Record.MutablePartitionResponse()->MutableCmdReadResult()->CopyFrom(rows);

    runtime->Send(new IEventHandle(proxy, setup.Context.Edge, response.Release()));
    auto followup = runtime->GrabEdgeEvent<TEvPersQueue::TEvRequest>(TDuration::Seconds(5));
    UNIT_ASSERT(followup);
    const auto& follow = followup->Record.GetPartitionRequest().GetCmdRead();
    UNIT_ASSERT_VALUES_EQUAL(follow.GetOffset(), 15);
    UNIT_ASSERT_VALUES_EQUAL(follow.GetPartNo(), 1);

    // Follow-up response starts at the split message. The published cursor must
    // still cover the whole staged batch, offsets 13..15.
    auto tail = MakeHolder<TEvPersQueue::TEvResponse>();
    tail->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    tail->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    auto* tailResult = tail->Record.MutablePartitionResponse()->MutableCmdReadResult();
    tailResult->SetRealReadOffset(15);
    tailResult->SetLastOffset(15);
    tailResult->SetEndOffset(24);
    auto* tailRow = tailResult->AddResult();
    tailRow->SetOffset(15);
    tailRow->SetPartNo(1);
    tailRow->SetTotalParts(2);
    tailRow->SetData("y");

    runtime->SetEventFilter([](NActors::TTestActorRuntimeBase&, TAutoPtr<NActors::IEventHandle>& ev) {
        return ev->CastAsLocal<TEvPQ::TEvStageDirectReadData>() != nullptr;
    });
    runtime->Send(new IEventHandle(proxy, tablet, tail.Release()));
    auto preparedDirect = runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(setup.Context.Edge, TDuration::Seconds(5));
    UNIT_ASSERT(preparedDirect && preparedDirect->Get());
    const auto& prepare = preparedDirect->Get()->Record.GetPartitionResponse().GetCmdPrepareReadResult();
    UNIT_ASSERT_VALUES_EQUAL(prepare.GetReadOffset(), 13);
    UNIT_ASSERT_VALUES_EQUAL(prepare.GetLastOffset(), 15);
    runtime->SetEventFilter(&NActors::TTestActorRuntimeBase::DefaultFilterFunc);

    read->ClearDirectReadId();
    auto plain = runtime->Register(CreateReadProxy(
            setup.Context.Edge, 1, tablet, 1, TDirectReadKey{}, request, TActorId{}));
    {
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back(TEvents::TEvBootstrap::EventType, 1);
        runtime->DispatchEvents(opts);
    }
    auto plainResponse = MakeHolder<TEvPersQueue::TEvResponse>();
    plainResponse->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    plainResponse->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    plainResponse->Record.MutablePartitionResponse()->MutableCmdReadResult()->CopyFrom(rows);
    runtime->Send(new IEventHandle(plain, setup.Context.Edge, plainResponse.Release()));
    auto prepared = runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(TDuration::Seconds(5));
    UNIT_ASSERT(prepared);
    const auto& plainRows = prepared->Record.GetPartitionResponse().GetCmdReadResult();
    UNIT_ASSERT_VALUES_EQUAL(plainRows.ResultSize(), 2);
    UNIT_ASSERT_VALUES_EQUAL(plainRows.GetResult(0).GetOffset(), 13);
    UNIT_ASSERT_VALUES_EQUAL(plainRows.GetResult(1).GetOffset(), 14);
}

// Sends the follow-up answer and returns the published prepare cursor.
// A second follow-up is dropped so a retry loop cannot spin the runtime.
// The stage event goes to a node-0 service id, which this runtime aborts on,
// so it is dropped here as well.
struct TPublishedPrepare {
    ui64 ReadOffset = 0;
    ui64 LastOffset = 0;
};

TPublishedPrepare FinishFollowUp(
        TTestActorRuntime& runtime,
        const TActorId& proxy,
        const TActorId& tablet,
        const TActorId& clientEdge,
        THolder<TEvPersQueue::TEvResponse> followUp)
{
    int extraFollowups = 0;
    runtime.SetEventFilter([&](NActors::TTestActorRuntimeBase&, TAutoPtr<NActors::IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvPersQueue::TEvRequest::EventType) {
            ++extraFollowups;
            return true;
        }
        return ev->CastAsLocal<TEvPQ::TEvStageDirectReadData>() != nullptr;
    });
    runtime.Send(new IEventHandle(proxy, tablet, followUp.Release()));
    auto prepared = runtime.GrabEdgeEvent<TEvPersQueue::TEvResponse>(clientEdge, TDuration::Seconds(5));
    runtime.SetEventFilter(&NActors::TTestActorRuntimeBase::DefaultFilterFunc);

    UNIT_ASSERT_VALUES_EQUAL(extraFollowups, 0);
    UNIT_ASSERT(prepared && prepared->Get());
    const auto& prepare = prepared->Get()->Record.GetPartitionResponse().GetCmdPrepareReadResult();
    return {prepare.GetReadOffset(), prepare.GetLastOffset()};
}

TActorId StartDirectRead(
        TTestSetup& setup,
        TTestActorRuntime& runtime,
        NKikimrClient::TPersQueueRequest& request,
        const TActorId& tablet)
{
    auto proxy = runtime.Register(CreateReadProxy(
            setup.Context.Edge, 1, tablet, 1, TDirectReadKey{"session1", 1, 1}, request, TActorId{}));
    TDispatchOptions opts;
    opts.FinalEvents.emplace_back(TEvents::TEvBootstrap::EventType, 1);
    runtime.DispatchEvents(opts);
    return proxy;
}

void SendTabletReadResult(
        TTestActorRuntime& runtime,
        const TActorId& proxy,
        const TActorId& sender,
        const NKikimrClient::TCmdReadResult& rows)
{
    auto response = MakeHolder<TEvPersQueue::TEvResponse>();
    response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    response->Record.MutablePartitionResponse()->MutableCmdReadResult()->CopyFrom(rows);
    runtime.Send(new IEventHandle(proxy, sender, response.Release()));
}

Y_UNIT_TEST(DirectReadEmptyStageCursorCanBeRestored) {
    // The tablet returned no rows for offset 40, and its own LastOffset is 100.
    // The next read must still retry 40. Restore requires ReadOffset <= LastOffset;
    // publishing 40 and 39 aborts the partition actor.
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    runtime->SetScheduledLimit(100000);

    NKikimrClient::TPersQueueRequest request;
    auto* read = request.MutablePartitionRequest()->MutableCmdRead();
    read->SetClientId("user");
    read->SetSessionId("session1");
    read->SetOffset(40);
    read->SetPartNo(0);
    read->SetDirectReadId(1);
    read->SetReadToBlobEnd(true);

    const auto tablet = runtime->AllocateEdgeActor();
    auto proxy = StartDirectRead(setup, *runtime, request, tablet);

    NKikimrClient::TCmdReadResult rows;
    rows.SetRealReadOffset(40);
    rows.SetLastOffset(100);
    rows.SetEndOffset(100);
    auto response = MakeHolder<TEvPersQueue::TEvResponse>();
    response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    response->Record.MutablePartitionResponse()->MutableCmdReadResult()->CopyFrom(rows);

    const auto published = FinishFollowUp(*runtime, proxy, tablet, setup.Context.Edge, std::move(response));
    UNIT_ASSERT(published.ReadOffset <= published.LastOffset);
    UNIT_ASSERT_VALUES_EQUAL(published.ReadOffset, 39);
    UNIT_ASSERT_VALUES_EQUAL(published.LastOffset, 39);
}

Y_UNIT_TEST(DirectReadFollowUpPublishesWhenTailIsGone) {
    // Offsets 13 and 14 are complete. Offset 15 is only part 0.
    // The follow-up does not return part 1: compaction left a new message at 16.
    // The proxy must publish 13..14 and skip 15, instead of requesting part 1 again.
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    runtime->SetScheduledLimit(100000);

    NKikimrClient::TPersQueueRequest request;
    auto* read = request.MutablePartitionRequest()->MutableCmdRead();
    read->SetClientId("user");
    read->SetSessionId("session1");
    read->SetOffset(13);
    read->SetPartNo(0);
    read->SetDirectReadId(1);
    read->SetReadToBlobEnd(true);

    const auto tablet = runtime->AllocateEdgeActor();
    auto proxy = StartDirectRead(setup, *runtime, request, tablet);

    NKikimrClient::TCmdReadResult rows;
    rows.SetRealReadOffset(13);
    rows.SetLastOffset(15);
    rows.SetEndOffset(24);
    auto add = [&](ui64 offset, ui32 partNo, ui32 totalParts) {
        auto* row = rows.AddResult();
        row->SetOffset(offset);
        row->SetData("x");
        row->SetPartNo(partNo);
        row->SetTotalParts(totalParts);
    };
    add(13, 0, 1);
    add(14, 0, 1);
    add(15, 0, 2);
    SendTabletReadResult(*runtime, proxy, setup.Context.Edge, rows);

    auto followup = runtime->GrabEdgeEvent<TEvPersQueue::TEvRequest>(TDuration::Seconds(5));
    UNIT_ASSERT(followup);
    const auto& follow = followup->Record.GetPartitionRequest().GetCmdRead();
    UNIT_ASSERT_VALUES_EQUAL(follow.GetOffset(), 15);
    UNIT_ASSERT_VALUES_EQUAL(follow.GetPartNo(), 1);

    NKikimrClient::TCmdReadResult tailGone;
    tailGone.SetRealReadOffset(16);
    tailGone.SetLastOffset(16);
    tailGone.SetEndOffset(24);
    auto* next = tailGone.AddResult();
    next->SetOffset(16);
    next->SetPartNo(0);
    next->SetTotalParts(1);
    next->SetData("z");
    auto tail = MakeHolder<TEvPersQueue::TEvResponse>();
    tail->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    tail->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    tail->Record.MutablePartitionResponse()->MutableCmdReadResult()->CopyFrom(tailGone);

    const auto published = FinishFollowUp(*runtime, proxy, tablet, setup.Context.Edge, std::move(tail));
    UNIT_ASSERT_VALUES_EQUAL(published.ReadOffset, 13);
    UNIT_ASSERT_VALUES_EQUAL(published.LastOffset, 15);
}

Y_UNIT_TEST(DirectReadFollowUpPublishesWhenTailPartIsEmpty) {
    // The only staged row is an incomplete message, and its tail comes back empty.
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    runtime->SetScheduledLimit(100000);

    NKikimrClient::TPersQueueRequest request;
    auto* read = request.MutablePartitionRequest()->MutableCmdRead();
    read->SetClientId("user");
    read->SetSessionId("session1");
    read->SetOffset(15);
    read->SetPartNo(0);
    read->SetDirectReadId(1);
    read->SetReadToBlobEnd(true);

    const auto tablet = runtime->AllocateEdgeActor();
    auto proxy = StartDirectRead(setup, *runtime, request, tablet);

    NKikimrClient::TCmdReadResult rows;
    rows.SetRealReadOffset(15);
    rows.SetLastOffset(15);
    rows.SetEndOffset(24);
    auto* row = rows.AddResult();
    row->SetOffset(15);
    row->SetData("x");
    row->SetPartNo(0);
    row->SetTotalParts(2);
    SendTabletReadResult(*runtime, proxy, setup.Context.Edge, rows);

    auto followup = runtime->GrabEdgeEvent<TEvPersQueue::TEvRequest>(TDuration::Seconds(5));
    UNIT_ASSERT(followup);
    UNIT_ASSERT_VALUES_EQUAL(followup->Record.GetPartitionRequest().GetCmdRead().GetPartNo(), 1);

    NKikimrClient::TCmdReadResult emptyTail;
    emptyTail.SetRealReadOffset(15);
    emptyTail.SetLastOffset(15);
    emptyTail.SetEndOffset(24);
    auto* missing = emptyTail.AddResult();
    missing->SetOffset(15);
    missing->SetPartNo(1);
    missing->SetTotalParts(2);
    auto tail = MakeHolder<TEvPersQueue::TEvResponse>();
    tail->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    tail->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    tail->Record.MutablePartitionResponse()->MutableCmdReadResult()->CopyFrom(emptyTail);

    const auto published = FinishFollowUp(*runtime, proxy, tablet, setup.Context.Edge, std::move(tail));
    UNIT_ASSERT_VALUES_EQUAL(published.ReadOffset, 15);
    UNIT_ASSERT_VALUES_EQUAL(published.LastOffset, 15);
}

Y_UNIT_TEST(TestPublishAndForget) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData());
    UNIT_ASSERT(resp->Data.empty());

    {
        auto* reg = new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 1);
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 1));
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT(resp->Data[0].second.Reads.empty());
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    {
        auto* reg = new TEvPQ::TEvPublishDirectRead(
                {"session1", 1, 1},
                1
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 1));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 1);
    {
        auto* reg = new TEvPQ::TEvForgetDirectRead(
                {"session1", 1, 1}, 1
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 1));
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 0);
}

Y_UNIT_TEST(TestDeregister) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    {
        auto* reg = new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 1);
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    {
        auto* reg = new TEvPQ::TEvRegisterDirectReadSession({"session2", 1}, 1);
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData(
            {"session1", 1}, 1)
    );
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT(resp->Data[0].second.Reads.empty());
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData());
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 2);
    {
        auto* reg = new TEvPQ::TEvDeregisterDirectReadSession({"session1", 1}, 1);
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData());
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
}

Y_UNIT_TEST(TestWrongSessionOrGeneration) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 2)
    );
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 1}, 2, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session1", 1, 1}, 2)
    );

    // Session with old id, shold not have any effect
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 1)
    );
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session1", 1, 1}, 1)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 1));
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 0);

    // Forget with old generation, should have no effect
    runtime->Send(
        setup.ProxyId, TActorId{},
        new TEvPQ::TEvForgetDirectRead({"session1", 1, 1}, 1)
    );

    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 2));
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 1);

    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session-2", 1}, 2), false);
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 99}, 2), false);
}

Y_UNIT_TEST(OutdatedSession) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 1)
    );
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session1", 1, 1}, 1)
    );

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 2)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 1));
    UNIT_ASSERT(resp->Data.empty());
}


Y_UNIT_TEST(MultipleSessions) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 1)
    );
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 2}, 1, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session1", 1, 1}, 1)
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session1", 1, 2}, 1)
    );

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session2", 1}, 2)
    );
     {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session2", 1, 3}, 2, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session2", 1, 3}, 2)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData());
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 2);
    for (const auto& [key, data] : resp->Data) {
        if (key.SessionId == "session1") {
            UNIT_ASSERT_VALUES_EQUAL(data.Generation, 1);
            UNIT_ASSERT_VALUES_EQUAL(data.Reads.size(), 2);
            auto iter = data.Reads.begin();
            UNIT_ASSERT_VALUES_EQUAL(iter->first, 1);
            UNIT_ASSERT_VALUES_EQUAL((++iter)->first, 2);
        }else if (key.SessionId == "session2") {
            UNIT_ASSERT_VALUES_EQUAL(data.Generation, 2);
            UNIT_ASSERT_VALUES_EQUAL(data.Reads.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(data.Reads.begin()->first, 3);
        }
    }
}

// LOGBROKER-10590: Stage/Publish may arrive before Register (fire-and-forget CreateSession).
// Buffer them and apply on Register so tablet inFlight is not stranded without cache data.
Y_UNIT_TEST(TestStagePublishBeforeRegister) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvStageDirectReadData(
                    {"session", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>())
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session", 1, 1}, 1)
    );

    // Not registered yet — nothing visible.
    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session", 1}, 1), /*status=*/false);
    UNIT_ASSERT(resp->Error);

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 1)
    );

    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session", 1}, 1));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.begin()->first, 1);
}

// Late Stage/Publish after Deregister must not be buffered (retired-generation tombstone).
Y_UNIT_TEST(TestLateStageAfterDeregisterNotBuffered) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 1)
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvDeregisterDirectReadSession({"session", 1}, 1)
    );

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvStageDirectReadData(
                    {"session", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>())
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session", 1, 1}, 1)
    );

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 2)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session", 1}, 2));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 0);
}

// Stage-before-Register pending must be dropped if Deregister arrives without Register.
Y_UNIT_TEST(TestPendingDroppedOnDeregisterWithoutRegister) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvStageDirectReadData(
                    {"session", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>())
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session", 1, 1}, 1)
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvDeregisterDirectReadSession({"session", 1}, 1)
    );

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 1)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session", 1}, 1));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 0);
}

// Stale lower-gen Deregister must not wipe Stage/Publish pending for a newer generation.
Y_UNIT_TEST(TestStaleDeregisterDoesNotDropNewerPending) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvStageDirectReadData(
                    {"session", 1, 1}, 2, std::make_shared<NKikimrClient::TResponse>())
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session", 1, 1}, 2)
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvDeregisterDirectReadSession({"session", 1}, 1)
    );

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 2)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session", 1}, 2));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.begin()->first, 1);
}

// Same readId from a late lower generation must not overwrite a newer pending Stage/Publish.
Y_UNIT_TEST(TestPendingKeepsHigherGenerationOnSameReadId) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvStageDirectReadData(
                    {"session", 1, 1}, 2, std::make_shared<NKikimrClient::TResponse>())
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session", 1, 1}, 2)
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvStageDirectReadData(
                    {"session", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>())
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session", 1, 1}, 1)
    );

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 2)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session", 1}, 2));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.begin()->first, 1);
}

// Stale lower-gen Forget must not drop newer pending Stage/Publish for the same readId.
Y_UNIT_TEST(TestStaleForgetDoesNotDropNewerPending) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvStageDirectReadData(
                    {"session", 1, 1}, 2, std::make_shared<NKikimrClient::TResponse>())
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session", 1, 1}, 2)
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvForgetDirectRead({"session", 1, 1}, 1)
    );

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 2)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session", 1}, 2));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.begin()->first, 1);
}

// Register of a lower generation must not discard pending Stage/Publish for a higher generation.
Y_UNIT_TEST(TestFlushKeepsHigherGenerationPending) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvStageDirectReadData(
                    {"session", 1, 1}, 3, std::make_shared<NKikimrClient::TResponse>())
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session", 1, 1}, 3)
    );

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 2)
    );
    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session", 1}, 2));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 0);

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 3)
    );
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session", 1}, 3));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.begin()->first, 1);
}

void AdvancePastDeadlineMapTtl(TTestActorRuntime* runtime) {
    // Default TTL is 5 minutes; wakeup period is 1 minute.
    for (int i = 0; i < 6; ++i) {
        runtime->AdvanceCurrentTime(TDuration::Minutes(1));
        runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
    }
}

// Pending Stage/Publish without Register must expire by TTL instead of leaking forever.
Y_UNIT_TEST(TestPendingExpiresByTtl) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvStageDirectReadData(
                    {"session", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>())
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session", 1, 1}, 1)
    );

    AdvancePastDeadlineMapTtl(runtime);

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 1)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session", 1}, 1));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 0);
}

// After retired tombstone expires, Stage-before-Register may buffer again.
Y_UNIT_TEST(TestRetiredExpiresAllowsBufferAgain) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 1)
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvDeregisterDirectReadSession({"session", 1}, 1)
    );

    AdvancePastDeadlineMapTtl(runtime);

    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvStageDirectReadData(
                    {"session", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>())
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session", 1, 1}, 1)
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session", 1}, 1)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session", 1}, 1));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.begin()->first, 1);
}
} // Test suite

} //namespace NKikimr::NPQ
