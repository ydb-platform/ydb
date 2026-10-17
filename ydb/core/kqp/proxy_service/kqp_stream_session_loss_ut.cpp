#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/proxy_service/kqp_proxy_service.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>

namespace NKikimr::NKqp {
namespace {

// A query RPC on node A runs a query in a session that lives on node B; result parts go from B to A.
class TRemoteSessionFixture {
public:
    // a is the node index of the RPC, b the one of the session
    explicit TRemoteSessionFixture(ui32 a = 0, ui32 b = 1)
        : A(a)
        , B(b)
    {
        auto settings = Tests::TServerSettings(Ports.GetPort(2134))
            .SetDomainName("Root")
            .SetNodeCount(2)
            .SetUseRealThreads(false);
        Server = new Tests::TServer(settings);
        Runtime = Server->GetRuntime();
        const auto sender = Runtime->AllocateEdgeActor();
        InitRoot(Server, sender);
        for (ui32 index = 0; index < 2; ++index) {
            Runtime->EnableScheduleForActor(Runtime->GetLocalServiceId(Proxy(index), index));
        }
        Rpc = Runtime->AllocateEdgeActor(A);
        Attached = Runtime->AllocateEdgeActor(B);
    }

    TActorId Proxy(ui32 nodeIndex) const {
        return MakeKqpProxyID(Runtime->GetNodeId(nodeIndex));
    }

    // creates a session on node B and attaches it there, as the query service does
    TString CreateAttachedSession() {
        auto request = MakeHolder<TEvKqp::TEvCreateSessionRequest>();
        request->Record.MutableRequest()->SetDatabase("/Root");
        Runtime->Send(new IEventHandle(Proxy(B), Attached, request.Release()), B);
        auto created = Runtime->GrabEdgeEventRethrow<TEvKqp::TEvCreateSessionResponse>(Attached);
        UNIT_ASSERT_VALUES_EQUAL(created->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        const TString sessionId = created->Get()->Record.GetResponse().GetSessionId();

        auto attach = MakeHolder<TEvKqp::TEvPingSessionRequest>();
        attach->Record.MutableRequest()->SetSessionId(sessionId);
        ActorIdToProto(Attached, attach->Record.MutableRequest()->MutableExtSessionCtrlActorId());
        Runtime->Send(new IEventHandle(Proxy(B), Attached, attach.Release()), B);
        auto attached = Runtime->GrabEdgeEventRethrow<TEvKqp::TEvPingSessionResponse>(Attached);
        UNIT_ASSERT_VALUES_EQUAL(attached->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        return sessionId;
    }

    // sends the query to the proxy of node A, as the RPC actor there does
    void SendQuery(const TString& sessionId, const TString& query, ui64 chunkSize) {
        SendQuery(sessionId, query, chunkSize, Rpc);
    }

    void SendQuery(const TString& sessionId, const TString& query, ui64 chunkSize, TActorId rpc) {
        auto request = MakeHolder<TEvKqp::TEvQueryRequest>();
        auto& record = *request->Record.MutableRequest();
        record.SetSessionId(sessionId);
        record.SetDatabase("/Root");
        record.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        record.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        record.SetQuery(query);
        record.SetOutputChunkMaxSize(chunkSize);
        ActorIdToProto(rpc, request->Record.MutableRequestActorId());
        Runtime->Send(new IEventHandle(Proxy(A), rpc, request.Release()), A);
    }

    // runs a query in an implicit session on node B and waits for its result
    void Exec(const TString& query) {
        const TActorId edge = Runtime->AllocateEdgeActor(B);
        auto request = MakeHolder<TEvKqp::TEvQueryRequest>();
        auto& record = *request->Record.MutableRequest();
        record.SetDatabase("/Root");
        record.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        record.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        record.SetQuery(query);
        ActorIdToProto(edge, request->Record.MutableRequestActorId());
        Runtime->Send(new IEventHandle(Proxy(B), edge, request.Release()), B);
        for (;;) {
            TAutoPtr<IEventHandle> handle;
            auto events = Runtime->GrabEdgeEventsRethrow<TEvKqp::TEvQueryResponse, TEvKqpExecuter::TEvStreamData>(handle);
            if (auto* response = std::get<TEvKqp::TEvQueryResponse*>(events)) {
                UNIT_ASSERT_VALUES_EQUAL_C(response->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
                    response->Record.ShortDebugString());
                return;
            }
            auto* data = std::get<TEvKqpExecuter::TEvStreamData*>(events);
            auto ack = MakeHolder<TEvKqpExecuter::TEvStreamDataAck>(data->Record.GetSeqNo(), data->Record.GetChannelId());
            ack->Record.SetFreeSpace(1 << 20);
            Runtime->Send(new IEventHandle(handle->Sender, edge, ack.Release()), B);
        }
    }

    const ui32 A;
    const ui32 B;
    TPortManager Ports;
    Tests::TServer::TPtr Server;
    TTestActorRuntime* Runtime = nullptr;
    TActorId Rpc; // the query RPC, on node A
    TActorId Attached; // the attach stream of the session, on node B
};

// The result the RPC actor gets: rows in the order they came and the final status.
struct TStreamResult {
    std::vector<ui64> Rows;
    std::vector<ui64> SeqNos;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
};

// acts like the query RPC: takes every part, acknowledges it, and finishes on the query response
TStreamResult ReceiveStream(TTestActorRuntime& runtime, TActorId rpc, TDuration timeout, ui32 rpcNode = 0) {
    TStreamResult result;
    const TInstant deadline = runtime.GetCurrentTime() + timeout;
    while (runtime.GetCurrentTime() < deadline) {
        TAutoPtr<IEventHandle> handle;
        auto events = runtime.GrabEdgeEventsRethrow<TEvKqp::TEvQueryResponse, TEvKqpExecuter::TEvStreamData>(handle,
            deadline - runtime.GetCurrentTime());
        if (auto* response = std::get<TEvKqp::TEvQueryResponse*>(events)) {
            result.Status = response->Record.GetYdbStatus();
            return result;
        }
        auto* data = std::get<TEvKqpExecuter::TEvStreamData*>(events);
        if (!data) {
            break;
        }
        const auto& record = data->Record;
        result.SeqNos.push_back(record.GetSeqNo());
        for (const auto& row : record.GetResultSet().rows()) {
            result.Rows.push_back(row.items(0).uint64_value());
        }
        auto ack = MakeHolder<TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
        ack->Record.SetFreeSpace(1 << 20);
        runtime.Send(new IEventHandle(handle->Sender, rpc, ack.Release()), rpcNode);
    }
    return result;
}

constexpr ui64 NumRows = 1000;
const TString Query = TStringBuilder()
    << "SELECT x FROM AS_TABLE(ListMap(ListFromRange(0ul, " << NumRows << "ul), ($x) -> (<|x: $x|>))) ORDER BY x;";

} // namespace

Y_UNIT_TEST_SUITE(KqpStreamSessionLoss) {

    // control: without network faults, the RPC on node A gets every row of a query in a session on node B
    Y_UNIT_TEST(RemoteSessionDeliversAllRows) {
        TRemoteSessionFixture fixture;
        const TString sessionId = fixture.CreateAttachedSession();
        fixture.SendQuery(sessionId, Query, 512);
        const auto result = ReceiveStream(*fixture.Runtime, fixture.Rpc, TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result.Rows.size(), NumRows);
        for (ui64 i = 0; i < NumRows; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(result.Rows[i], i);
        }
        UNIT_ASSERT_C(result.SeqNos.size() > 2, "the result must come in several parts");
    }

    // A query reads a table through a session on node B, and its RPC is on node A. The shard is slow, so the result
    // comes over a long time. The network from B to A stalls after the RPC has a few parts, the next parts wait in it,
    // and the session between the nodes breaks, losing them; then the nodes connect again. The RPC must get either
    // every row or an error, not a successful result with a gap, and it must get it without waiting for the query
    // timeout.
    //
    // The faults keep the order of events: the shard takes the events sent to it one at a time and in order, the link
    // stalls as a whole, and the break drops what it holds, as an interconnect session does.
    //
    // lostTail: the shard reads to the end before the break, so the end of the result and the final response are lost
    // slowProxy: the KQP proxy of node A is slow from the break until the query ends, so the response that comes
    // through the new session waits in its mailbox together with the news of the break
    // newQuery: with slowProxy, another query comes to the proxy of node A before the news of the break, so it goes
    // through the new session; the news about the old one must not fail it
    void CheckLostPart(ui32 a, ui32 b, bool lostTail, bool slowProxy = false, bool newQuery = false) {
        TRemoteSessionFixture fixture(a, b);
        auto& runtime = *fixture.Runtime;
        runtime.SetDispatchedEventsLimit(10'000'000);
        runtime.SetScheduledLimit(10'000'000);
        constexpr ui64 numRows = 20000;
        fixture.Exec("CREATE TABLE `/Root/T` (k Uint64, v Uint64, PRIMARY KEY (k)) "
            "WITH (UNIFORM_PARTITIONS = 1, AUTO_PARTITIONING_BY_SIZE = DISABLED);");
        fixture.Exec(TStringBuilder() << "UPSERT INTO `/Root/T` SELECT x AS k, x AS v FROM AS_TABLE(ListMap("
            << "ListFromRange(0ul, " << numRows << "ul), ($x) -> (<|x: $x|>)));");
        const TString sessionId = fixture.CreateAttachedSession();
        const TString newSessionId = newQuery ? fixture.CreateAttachedSession() : TString();
        const TActorId newRpc = runtime.AllocateEdgeActor(fixture.A);

        // the shard returns the table in many reads, as for a big table, so the query still has rows to send after
        // the break
        auto savedRead = GetDefaultReadSettings();
        auto savedReadAck = GetDefaultReadAckSettings();
        Y_DEFER {
            SetDefaultReadSettings(savedRead->Record);
            SetDefaultReadAckSettings(savedReadAck->Record);
        };
        {
            NKikimrTxDataShard::TEvRead read;
            read.SetMaxRowsInResult(100);
            read.SetMaxRows(100);
            SetDefaultReadSettings(read);
            NKikimrTxDataShard::TEvReadAck ack;
            ack.SetMaxRows(100);
            SetDefaultReadAckSettings(ack);
        }

        const TActorId proxyOnA = runtime.GetInterconnectProxy(fixture.A, fixture.B); // receives what node B sends to node A
        const ui32 injectType = EventSpaceBegin(TEvents::ES_PRIVATE); // a batch of a session of the interconnect mock
        auto isFromB = [&](const IEventHandle& ev) {
            return ev.GetTypeRewrite() == injectType && !ev.Sender
                && (ev.Recipient == proxyOnA || ev.GetRecipientRewrite() == proxyOnA);
        };

        const TActorId kqpProxyA = runtime.GetLocalServiceId(fixture.Proxy(fixture.A), fixture.A);
        bool kqpProxyPaused = false;
        std::deque<std::unique_ptr<IEventHandle>> heldKqpProxy; // waiting for the slow KQP proxy of node A
        bool linkPaused = false;
        bool shardsPaused = false;
        std::set<TActorId> shards; // tablets the query reads
        std::deque<std::unique_ptr<IEventHandle>> heldShards; // waiting for slow shards
        std::deque<std::unique_ptr<IEventHandle>> heldLink; // in the network from B to A
        const IEventHandle* releasing = nullptr;
        ui32 partsFromExecuter = 0;
        const ui32 nodeB = runtime.GetNodeId(fixture.B);
        ui32 responsesOnBWhileProxyPaused = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev.Get() == releasing) {
                releasing = nullptr;
                return TTestActorRuntime::EEventAction::PROCESS;
            }
            if (kqpProxyPaused && ev->GetTypeRewrite() == TEvKqp::TEvQueryResponse::EventType
                && ev->Sender.NodeId() == nodeB && ev->GetRecipientRewrite().NodeId() == nodeB)
            {
                ++responsesOnBWhileProxyPaused; // a session actor of node B ends a query
            }
            if (ev->GetTypeRewrite() == TEvKqpExecuter::TEvStreamData::EventType && ev->Recipient != fixture.Rpc) {
                ++partsFromExecuter; // a part leaves the executer for the session actor
            }
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                shards.insert(ev->GetRecipientRewrite());
            }
            if (shardsPaused && shards.contains(ev->GetRecipientRewrite())) {
                heldShards.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (linkPaused && isFromB(*ev)) {
                heldLink.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (kqpProxyPaused && (ev->GetRecipientRewrite() == kqpProxyA || ev->Recipient == fixture.Proxy(fixture.A))) {
                heldKqpProxy.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc); };

        auto releaseShardEvent = [&] {
            auto ev = std::move(heldShards.front());
            heldShards.pop_front();
            releasing = ev.get();
            const ui32 nodeIndex = ev->GetRecipientRewrite().NodeId() - runtime.GetNodeId(0);
            runtime.Send(ev.release(), nodeIndex, true);
        };

        TStreamResult result;
        ui32 partsReceived = 0;
        // the RPC takes the parts that have reached it and acknowledges them
        auto takeParts = [&] {
            for (;;) {
                TAutoPtr<IEventHandle> handle;
                auto* data = runtime.GrabEdgeEventRethrow<TEvKqpExecuter::TEvStreamData>(handle, TDuration::MicroSeconds(1));
                if (!data) {
                    return;
                }
                ++partsReceived;
                for (const auto& row : data->Record.GetResultSet().rows()) {
                    result.Rows.push_back(row.items(0).uint64_value());
                }
                auto ack = MakeHolder<TEvKqpExecuter::TEvStreamDataAck>(data->Record.GetSeqNo(), data->Record.GetChannelId());
                ack->Record.SetFreeSpace(1 << 20);
                runtime.Send(new IEventHandle(handle->Sender, fixture.Rpc, ack.Release()), fixture.A, true);
            }
        };

        // The shard is slow, it takes one event at a time, so the query reads the table for a long time. The link
        // works until the RPC has a few parts, then it stalls as a whole, and the next parts wait in it.
        shardsPaused = true;
        fixture.SendQuery(sessionId, "SELECT k FROM `/Root/T`;", 512);
        for (ui32 i = 0; i < 100000 && partsReceived < 3; ++i) {
            if (!heldShards.empty()) {
                releaseShardEvent();
            }
            runtime.SimulateSleep(TDuration::MicroSeconds(100));
            takeParts();
        }
        UNIT_ASSERT_C(partsReceived >= 3, "the RPC got too few parts");
        linkPaused = true;
        const ui32 partsAtStall = partsFromExecuter;
        if (lostTail) {
            // the shard reads to the end, so the rest of the result and the final response wait in the link
            for (ui32 idle = 0, i = 0; i < 100000 && idle < 100; ++i) {
                if (!heldShards.empty()) {
                    releaseShardEvent();
                    idle = 0;
                } else {
                    ++idle;
                }
                runtime.SimulateSleep(TDuration::MicroSeconds(100));
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
        } else {
            for (ui32 i = 0; i < 100000 && partsFromExecuter < partsAtStall + 3; ++i) {
                if (!heldShards.empty()) {
                    releaseShardEvent();
                }
                runtime.SimulateSleep(TDuration::MicroSeconds(100));
            }
        }
        takeParts();
        UNIT_ASSERT_C(!heldLink.empty(), "nothing waits in the stalled link");
        if (lostTail) {
            UNIT_ASSERT_C(heldShards.empty(), "the shard must have read the whole table before the break");
        } else {
            UNIT_ASSERT_C(!heldShards.empty(), "the query must still read the table at the break");
        }

        // the session breaks: what is in the network is lost; the nodes connect again
        kqpProxyPaused = slowProxy;
        if (newQuery) {
            fixture.SendQuery(newSessionId, "UPSERT INTO `/Root/T` (k, v) VALUES (1000000ul, 1ul);", 512, newRpc);
        }
        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(fixture.B, fixture.A), {}, new TEvInterconnect::TEvDisconnect()), fixture.B, true);
        heldLink.clear();
        linkPaused = false;
        runtime.SimulateSleep(TDuration::MilliSeconds(10));
        shardsPaused = false;
        while (!heldShards.empty()) {
            auto ev = std::move(heldShards.front());
            heldShards.pop_front();
            releasing = ev.get();
            const ui32 nodeIndex = ev->GetRecipientRewrite().NodeId() - runtime.GetNodeId(0);
            runtime.Send(ev.release(), nodeIndex, true);
        }
        if (slowProxy) {
            // the query ends on node B while the proxy of node A is slow; then the proxy takes its events in order
            runtime.SimulateSleep(TDuration::Seconds(1));
            auto position = [&](ui32 type) {
                for (size_t i = 0; i < heldKqpProxy.size(); ++i) {
                    if (heldKqpProxy[i]->GetTypeRewrite() == type) {
                        return i;
                    }
                }
                return heldKqpProxy.size();
            };
            UNIT_ASSERT_C(responsesOnBWhileProxyPaused, "the query must end on node B while the proxy of node A is slow");
            if (newQuery) {
                // the proxy may get no news of the break at all, if it does not subscribe to the session
                UNIT_ASSERT_C(position(TEvKqp::TEvQueryRequest::EventType)
                    < position(TEvInterconnect::TEvNodeDisconnected::EventType),
                    "the new query must come to the proxy before the news of the break");
            }
            kqpProxyPaused = false;
            while (!heldKqpProxy.empty()) {
                auto ev = std::move(heldKqpProxy.front());
                heldKqpProxy.pop_front();
                runtime.Send(ev.release(), fixture.A, true);
            }
        }
        if (newQuery) {
            auto response = runtime.GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(newRpc, TDuration::Minutes(1));
            UNIT_ASSERT_C(response, "the new query got no response");
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
                response->Get()->Record.ShortDebugString());
        }

        // much shorter than the default query timeout, 2 hours: the RPC must learn about the break, not wait it out
        const auto rest = ReceiveStream(runtime, fixture.Rpc, TDuration::Minutes(10), fixture.A);
        result.Rows.insert(result.Rows.end(), rest.Rows.begin(), rest.Rows.end());
        result.Status = rest.Status;
        const std::set<ui64> distinct(result.Rows.begin(), result.Rows.end());
        if (result.Status == Ydb::StatusIds::SUCCESS) {
            UNIT_ASSERT_VALUES_EQUAL_C(distinct.size(), numRows, "a successful result lost rows");
        }
        UNIT_ASSERT_C(result.Status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, "the RPC got no final response");
    }

    // parts in the middle of the result are lost while the query still reads the table, so it has later parts to send
    Y_UNIT_TEST(LostMiddleOnSessionBreakIsNotSuccess) {
        CheckLostPart(0, 1, false);
    }

    Y_UNIT_TEST(LostMiddleOnSessionBreakIsNotSuccessSessionOnFirstNode) {
        CheckLostPart(1, 0, false);
    }

    // the query ends on node B before the slow proxy of node A learns about the break; no successful response may come
    // to it through the new session
    Y_UNIT_TEST(LostMiddleOnSessionBreakIsNotSuccessSlowProxy) {
        CheckLostPart(0, 1, false, true);
    }

    // the news of the broken session must fail only the queries that went through it
    Y_UNIT_TEST(OldSessionBreakDoesNotFailNewQuery) {
        CheckLostPart(0, 1, false, true, true);
    }

    // the end of the result and the final response are lost
    Y_UNIT_TEST(LostTailOnSessionBreakIsError) {
        CheckLostPart(0, 1, true);
    }

    Y_UNIT_TEST(LostTailOnSessionBreakIsErrorSessionOnFirstNode) {
        CheckLostPart(1, 0, true);
    }

}

} // namespace NKikimr::NKqp
