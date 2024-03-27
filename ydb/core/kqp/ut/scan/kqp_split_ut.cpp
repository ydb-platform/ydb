#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/runtime/kqp_stream_lookup_actor.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <util/generic/size_literals.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <library/cpp/threading/future/async.h>

namespace NKikimr {
namespace NKqp {


Y_UNIT_TEST_SUITE(KqpSplit) {
    static ui64 RunSchemeTx(
            TTestActorRuntimeBase& runtime,
            THolder<TEvTxUserProxy::TEvProposeTransaction>&& request,
            TActorId sender = {},
            bool viaActorSystem = false,
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus expectedStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress)
    {
        if (!sender) {
            sender = runtime.AllocateEdgeActor();
        }

        runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, viaActorSystem);
        auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
        Cerr << (TStringBuilder() << "scheme op " << ev->Get()->Record.ShortDebugString()) << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), expectedStatus);

        return ev->Get()->Record.GetTxId();
    }

    ui64 AsyncSplitTable(
            Tests::TServer* server,
            TActorId sender,
            const TString& path,
            ui64 sourceTablet,
            ui64 splitKey)
    {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetExecTimeoutPeriod(Max<ui64>());

        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);

        auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableSplitMergeTablePartitions();
        desc.SetTablePath(path);
        desc.AddSourceTabletId(sourceTablet);
        desc.AddSplitBoundary()->MutableKeyPrefix()->AddTuple()->MutableOptional()->SetUint64(splitKey);

        return RunSchemeTx(*server->GetRuntime(), std::move(request), sender, true);
    }

    void WaitTxNotification(Tests::TServer* server, TActorId sender, ui64 txId) {
        auto &runtime = *server->GetRuntime();
        auto &settings = server->GetSettings();

        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        request->Record.SetTxId(txId);
        auto tid = NKikimr::Tests::ChangeStateStorage(NKikimr::Tests::SchemeRoot, settings.Domain);
        runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries());
        runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
    }

    i64 SetSplitMergePartCountLimit(TTestActorRuntime* runtime, i64 val) {
        TAtomic prev;
        runtime->GetAppData().Icb->SetValue("SchemeShard_SplitMergePartCountLimit", val, prev);
        return prev;
    }

    class TReplyPipeStub : public TActor<TReplyPipeStub> {
    public:
        TReplyPipeStub(TActorId owner, TActorId client)
            : TActor<TReplyPipeStub>(&TReplyPipeStub::State)
            , Owner(owner)
            , Client(client)
        {
        }

        STATEFN(State) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPipeCache::TEvForward, Handle);
                hFunc(TEvPipeCache::TEvUnlink, Handle);
                hFunc(TEvDataShard::TEvReadResult, Handle);
                default:
                    Handle(ev);
            }
        }

        void Handle(TAutoPtr<IEventHandle> ev) {
            Forward(ev, Client);
        }

        void Handle(TEvDataShard::TEvReadResult::TPtr& ev) {
            if (ToSkip.fetch_sub(1) <= 0 && ToCapture.fetch_sub(1) > 0) {
                Cerr << "captured evreadresult -----------------------------------------------------------" << Endl;
                with_lock(CaptureLock) {
                    Captured.push_back(THolder(ev.Release()));
                }
                if (ToCapture.load() <= 0) {
                    ReadsReceived.Signal();
                }
                return;
            }
            Send(Client, ev->ReleaseBase());
        }

        void Handle(TEvPipeCache::TEvForward::TPtr& ev) {
            Send(PipeCache, ev->Release());
        }

        void Handle(TEvPipeCache::TEvUnlink::TPtr& ev) {
            Send(PipeCache, ev->Release());
        }

        void SetupCapture(i64 skip, i64 capture) {
            ToCapture.store(capture);
            ToSkip.store(skip);
            ReadsReceived.Reset();
        }

        void SendCaptured(NActors::TTestActorRuntime* runtime) {
            TVector<THolder<IEventHandle>> tosend;
            with_lock(CaptureLock) {
                tosend.swap(Captured);
            }
            for (auto& ev : tosend) {
                ev->Rewrite(ev->GetTypeRewrite(), Client);
                runtime->Send(ev.Release());
            }
        }

    private:
        TActorId PipeCache = MakePipePeNodeCacheID(false);
        TActorId Owner;
        TActorId Client;

        TMutex CaptureLock;
        TVector<THolder<IEventHandle>> Captured;
        TManualEvent ReadsReceived;

        std::atomic<i64> ToCapture;
        std::atomic<i64> ToSkip;
    };

    class TReadActorPipeCacheStub : public TActor<TReadActorPipeCacheStub> {
    public:
        TReadActorPipeCacheStub()
            : TActor<TReadActorPipeCacheStub>(&TReadActorPipeCacheStub::State)
        {
            SkipAll();
            AllowResults();
        }

        void SetupResultsCapture(i64 skip, i64 capture = std::numeric_limits<i64>::max()) {
            ReverseSkip.store(skip);
            ReverseCapture.store(capture);
            for (auto& [_, pipe] : Pipes) {
                pipe->SetupCapture(ReverseSkip.load(), ReverseCapture.load());
            }
        }

        void AllowResults() {
            SetupResultsCapture(std::numeric_limits<i64>::max(), 0);
        }

        void SetupCapture(i64 skip, i64 capture = std::numeric_limits<i64>::max()) {
            ToCapture.store(capture);
            ToSkip.store(skip);
            ReadsReceived.Reset();
        }

        void SkipAll() {
            SetupCapture(std::numeric_limits<i64>::max(), 0);
        }

        void State(TAutoPtr<::NActors::IEventHandle> &ev) {
            if (ev->GetTypeRewrite() == TEvPipeCache::TEvForward::EventType) {
                auto* forw = reinterpret_cast<TEvPipeCache::TEvForward::TPtr*>(&ev);
                auto readtype = TEvDataShard::TEvRead::EventType;
                auto acktype = TEvDataShard::TEvReadAck::EventType;
                auto actual = forw->Get()->Get()->Ev->Type();
                bool isRead = actual == readtype || acktype;
                if (isRead && ToSkip.fetch_sub(1) <= 0 && ToCapture.fetch_sub(1) > 0) {
                    Cerr << "captured evread -----------------------------------------------------------" << Endl;
                    with_lock(CaptureLock) {
                        Captured.push_back(THolder(ev.Release()));
                    }
                    if (ToCapture.load() <= 0) {
                        ReadsReceived.Signal();
                    }
                    return;
                }
            }
            Forward(ev);
        }

        void Forward(TAutoPtr<::NActors::IEventHandle> ev) {
            TReplyPipeStub* pipe = Pipes[ev->Sender];
            if (pipe == nullptr) {
                pipe = Pipes[ev->Sender] = new TReplyPipeStub(SelfId(), ev->Sender);
                Register(pipe);
                for (auto& [_, pipe] : Pipes) {
                    pipe->SetupCapture(ReverseSkip.load(), ReverseCapture.load());
                }
            }
            auto id = pipe->SelfId();
            TActor<TReadActorPipeCacheStub>::Forward(ev, id);
        }

        void SendCaptured(NActors::TTestActorRuntime* runtime, bool sendResults = true) {
            TVector<THolder<IEventHandle>> tosend;
            with_lock(CaptureLock) {
                tosend.swap(Captured);
            }
            for (auto& ev : tosend) {
                TReplyPipeStub* pipe = Pipes[ev->Sender];
                if (pipe == nullptr) {
                    pipe = Pipes[ev->Sender] = new TReplyPipeStub(SelfId(), ev->Sender);
                    runtime->Register(pipe);
                    for (auto& [_, pipe] : Pipes) {
                        pipe->SetupCapture(ReverseSkip.load(), ReverseCapture.load());
                    }
                }
                auto id = pipe->SelfId();
                ev->Rewrite(ev->GetTypeRewrite(), id);
                runtime->Send(ev.Release());
            }
            if (sendResults) {
                for (auto& [_, pipe] : Pipes) {
                    pipe->SendCaptured(runtime);
                }
            }
        }

    public:
        TManualEvent ReadsReceived;
        std::atomic<i64> ToCapture;
        std::atomic<i64> ToSkip;

        std::atomic<i64> ReverseCapture;
        std::atomic<i64> ReverseSkip;

        TMutex CaptureLock;
        TVector<THolder<IEventHandle>> Captured;
        THashMap<TActorId, TReplyPipeStub*> Pipes;
    };

    TString ALL = ",101,102,103,201,202,203,301,302,303,401,402,403,501,502,503,601,602,603,701,702,703,801,802,803";
    TString Format(TVector<ui64> keys) {
        TStringBuilder res;
        for (auto k : keys) {
            res << "," << k;
        }
        return res;
    }

    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSQLRequest(const TString &sql, bool dml) {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        if (dml) {
            request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
            request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        }
        request->Record.SetRequestType("_document_api_request");
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(dml
                                                  ? NKikimrKqp::QUERY_TYPE_SQL_DML
                                                  : NKikimrKqp::QUERY_TYPE_SQL_DDL);
        request->Record.MutableRequest()->SetQuery(sql);
        request->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
        return request;
    }

    void SendSQL(Tests::TServer::TPtr server,
                 TActorId sender,
                 const TString &sql,
                 bool dml)
    {
        auto &runtime = *server->GetRuntime();
        auto request = MakeSQLRequest(sql, dml);
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));
    }

    void ExecSQL(NActors::TTestActorRuntime& runtime,
                 TActorId sender,
                 const TString &sql,
                 bool dml = true,
                 Ydb::StatusIds::StatusCode code = Ydb::StatusIds::SUCCESS)
    {
        TAutoPtr<IEventHandle> handle;

        auto request = MakeSQLRequest(sql, dml);
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetRef().GetYdbStatus(), code);
    }

    void SendScanQuery(TTestActorRuntime* runtime, TActorId kqpProxy, TActorId sender, const TString& queryText) {
        auto ev = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCAN);
        ev->Record.MutableRequest()->SetQuery(queryText);
        ev->Record.MutableRequest()->SetKeepSession(false);
        ActorIdToProto(sender, ev->Record.MutableRequestActorId());
        runtime->Send(new IEventHandle(kqpProxy, sender, ev.release()));
    };

    void CollectKeysTo(TVector<ui64>* collectedKeys, TTestActorRuntime* runtime, TActorId sender) {
        auto captureEvents = [=](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKqp::TEvKqpExecuter::TEvStreamData::EventType) {
                auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;
                for (auto& row : record.resultset().rows()) {
                    collectedKeys->push_back(row.items(0).uint64_value());
                }

                auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                resp->Record.SetEnough(false);
                resp->Record.SetSeqNo(record.GetSeqNo());
                runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                return true;
            }

            return false;
        };
        runtime->SetEventFilter(captureEvents);
    }

    enum class SortOrder {
        Descending,
        Ascending,
        Unspecified
    };

    TString OrderBy(SortOrder o) {
        if (o == SortOrder::Ascending) {
            return " ORDER BY Key ";
        }
        if (o == SortOrder::Descending) {
            return " ORDER BY Key DESC ";
        }
        return " ";
    }

    TVector<ui64> Canonize(TVector<ui64> collectedKeys, SortOrder o) {
        if (o == SortOrder::Unspecified) {
            Sort(collectedKeys);
        }
        if (o == SortOrder::Descending) {
            Reverse(collectedKeys.begin(), collectedKeys.end());
        }
        return collectedKeys;
    }

#define Y_UNIT_TEST_SORT(N, OPT)                                                                                   \
    template <SortOrder OPT>                                                                                       \
    struct TTestCase##N : public TCurrentTestCase {                                                                \
        TTestCase##N() : TCurrentTestCase() {                                                                      \
            if constexpr (OPT == SortOrder::Descending) { Name_ = #N "+Descending"; }                              \
            if constexpr (OPT == SortOrder::Ascending) { Name_ = #N "+Ascending"; }                                \
            if constexpr (OPT == SortOrder::Unspecified) { Name_ = #N "+Unspecified"; }                            \
        }                                                                                                          \
                                                                                                                   \
        static THolder<NUnitTest::TBaseTestCase> Create()  { return ::MakeHolder<TTestCase##N<Order>>();  }        \
        void Execute_(NUnitTest::TTestContext&) override;                                                          \
    };                                                                                                             \
    struct TTestRegistration##N {                                                                                  \
        TTestRegistration##N() {                                                                                   \
            TCurrentTest::AddTest(TTestCase##N<SortOrder::Ascending>::Create);                                     \
            TCurrentTest::AddTest(TTestCase##N<SortOrder::Descending>::Create);                                    \
            TCurrentTest::AddTest(TTestCase##N<SortOrder::Unspecified>::Create);                                   \
        }                                                                                                          \
    };                                                                                                             \
    static TTestRegistration##N testRegistration##N;                                                               \
    template <SortOrder OPT>                                                                                            \
    void TTestCase##N<OPT>::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

    enum class ETestActorType {
        SorceRead,
        StreamLookup
    };

    struct TTestSetup {
        TTestSetup(ETestActorType testActorType, TString table = "/Root/KeyValueLargePartition", Tests::TServer* providedServer = nullptr)
            : Table(table)
        {
            if (testActorType == ETestActorType::SorceRead) {
                InterceptReadActorPipeCache(MakePipePeNodeCacheID(false));
            } else if (testActorType == ETestActorType::StreamLookup) {
                InterceptStreamLookupActorPipeCache(MakePipePeNodeCacheID(false));
            }
            
            if (providedServer) {
                Server = providedServer;
            } else {
                TKikimrSettings settings;
                NKikimrConfig::TAppConfig appConfig;
                appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(testActorType == ETestActorType::SorceRead);
                appConfig.MutableTableServiceConfig()->SetEnableKqpScanQueryStreamLookup(testActorType == ETestActorType::StreamLookup);
                settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
                settings.SetAppConfig(appConfig);

                Kikimr.ConstructInPlace(settings);
                Server = &Kikimr->GetTestServer();
            }

            Runtime = Server->GetRuntime();
            KqpProxy = MakeKqpProxyID(Runtime->GetNodeId(0));

            {
                auto settings = MakeIntrusive<TIteratorReadBackoffSettings>();
                settings->StartRetryDelay = TDuration::MilliSeconds(250);
                settings->MaxShardAttempts = 4;
                SetReadIteratorBackoffSettings(settings);
            }

            Sender = Runtime->AllocateEdgeActor();

            if (providedServer) {
                InitRoot(Server, Sender);
            }

            CollectKeysTo(&CollectedKeys, Runtime, Sender);

            SetSplitMergePartCountLimit(Runtime, -1);
        }

        TVector<ui64> Shards() {
            return GetTableShards(Server, Sender, Table);
        }

        void Split(ui64 shard, ui32 key) {
            auto senderSplit = Runtime->AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(Server, senderSplit, Table, shard, key);
            WaitTxNotification(Server, senderSplit, txId);
        }

        void AssertSuccess() {
            auto reply = Runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(Sender);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        }

        void SendScanQuery(TString text) {
           ::NKikimr::NKqp::NTestSuiteKqpSplit::SendScanQuery(Runtime, KqpProxy, Sender, text);
        }

        TMaybe<TKikimrRunner> Kikimr;
        TVector<ui64> CollectedKeys;
        Tests::TServer* Server;
        NActors::TTestActorRuntime* Runtime;
        TActorId KqpProxy;
        TActorId Sender;

        TString Table;
    };

    Y_UNIT_TEST_SORT(AfterResolve, Order) {
        TTestSetup s(ETestActorType::SorceRead);

        auto shards = s.Shards();
        auto* shim = new TReadActorPipeCacheStub();
        InterceptReadActorPipeCache(s.Runtime->Register(shim));
        shim->SetupCapture(0, 1);
        s.SendScanQuery("SELECT Key FROM `/Root/KeyValueLargePartition`" + OrderBy(Order));

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        s.Split(shards.at(0), 400);
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->SendCaptured(s.Runtime);

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, Order)), ALL);
    }

    Y_UNIT_TEST_SORT(AfterResult, Order) {
        TTestSetup s(ETestActorType::SorceRead);
        auto shards = s.Shards();

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(8);
        evread.SetMaxRows(8);
        SetDefaultReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        evreadack.SetMaxRows(8);
        SetDefaultReadAckSettings(evreadack);

        auto* shim = new TReadActorPipeCacheStub();
        shim->SetupCapture(1, 1);
        shim->SetupResultsCapture(1);
        InterceptReadActorPipeCache(s.Runtime->Register(shim));
        s.SendScanQuery("SELECT Key FROM `/Root/KeyValueLargePartition`" + OrderBy(Order));

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        s.Split(shards.at(0), 400);
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->AllowResults();
        shim->SendCaptured(s.Runtime);

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, Order)), ALL);
    }

    const TString SegmentsResult = ",101,102,103,202,203,301,303,401,403,501,502,601,602,603,702,703,801";
    const TString SegmentsRequest =
            "SELECT Key FROM `/Root/KeyValueLargePartition` where \
            (Key >= 101 and Key <= 103) \
            or (Key >= 202 and Key <= 301) \
            or (Key >= 303 and Key <= 401) \
            or (Key >= 403 and Key <= 502) \
            or (Key >= 601 and Key <= 603) \
            or (Key >= 702 and Key <= 801) \
            ";

    Y_UNIT_TEST_SORT(AfterResultMultiRange, Order) {
        TTestSetup s(ETestActorType::SorceRead);
        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(5);
        evread.SetMaxRows(5);
        SetDefaultReadSettings(evread);

        auto shards = s.Shards();

        NKikimrTxDataShard::TEvReadAck evreadack;
        evreadack.SetMaxRows(5);
        SetDefaultReadAckSettings(evreadack);

        auto* shim = new TReadActorPipeCacheStub();
        shim->SetupCapture(1, 1);
        shim->SetupResultsCapture(1);
        InterceptReadActorPipeCache(s.Runtime->Register(shim));
        s.SendScanQuery(SegmentsRequest + OrderBy(Order));

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        s.Split(shards.at(0), 404);
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->AllowResults();
        shim->SendCaptured(s.Runtime);

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, Order)), SegmentsResult);
    }

    Y_UNIT_TEST_SORT(AfterResultMultiRangeSegmentPartition, Order) {
        TTestSetup s(ETestActorType::SorceRead);
        auto shards = s.Shards();

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(5);
        evread.SetMaxRows(5);
        SetDefaultReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        evreadack.SetMaxRows(5);
        SetDefaultReadAckSettings(evreadack);

        auto* shim = new TReadActorPipeCacheStub();
        shim->SetupCapture(1, 1);
        shim->SetupResultsCapture(1);
        InterceptReadActorPipeCache(s.Runtime->Register(shim));
        s.SendScanQuery(SegmentsRequest + OrderBy(Order));

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        s.Split(shards.at(0), 501);
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->AllowResults();
        shim->SendCaptured(s.Runtime);

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, Order)), SegmentsResult);
    }

    Y_UNIT_TEST_SORT(ChoosePartition, Order) {
        TTestSetup s(ETestActorType::SorceRead);
        auto shards = s.Shards();

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(8);
        evread.SetMaxRows(8);
        SetDefaultReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        evreadack.SetMaxRows(8);
        SetDefaultReadAckSettings(evreadack);

        auto* shim = new TReadActorPipeCacheStub();
        shim->SetupCapture(2, 1);
        shim->SetupResultsCapture(2);
        InterceptReadActorPipeCache(s.Runtime->Register(shim));
        s.SendScanQuery("SELECT Key FROM `/Root/KeyValueLargePartition`" + OrderBy(Order));

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        s.Split(shards.at(0), 400);
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->AllowResults();
        shim->SendCaptured(s.Runtime);

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, Order)), ALL);
    }


    Y_UNIT_TEST_SORT(BorderKeys, Order) {
        TTestSetup s(ETestActorType::SorceRead);
        auto shards = s.Shards();

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(12);
        evread.SetMaxRows(12);
        SetDefaultReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        evreadack.SetMaxRows(12);
        SetDefaultReadAckSettings(evreadack);

        auto* shim = new TReadActorPipeCacheStub();
        shim->SetupCapture(1, 1);
        shim->SetupResultsCapture(1);
        InterceptReadActorPipeCache(s.Runtime->Register(shim));
        s.SendScanQuery("SELECT Key FROM `/Root/KeyValueLargePartition`" + OrderBy(Order));

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;

        s.Split(shards.at(0), 402);
        shards = s.Shards();
        s.Split(shards.at(1), 404);

        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->AllowResults();
        shim->SendCaptured(s.Runtime);

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, Order)), ALL);
    }

    Y_UNIT_TEST_SORT(IntersectionLosesRange, Order) {
        TTestSetup s(ETestActorType::SorceRead);
        auto shards = s.Shards();

        auto* shim = new TReadActorPipeCacheStub();
        InterceptReadActorPipeCache(s.Runtime->Register(shim));
        shim->SetupCapture(0, 1);
        s.SendScanQuery("SELECT Key FROM `/Root/KeyValueLargePartition` where Key = 101 or (Key >= 202 and Key < 200+4) or (Key >= 701 and Key < 704)" + OrderBy(Order));
        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        s.Split(shards.at(0), 190);
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->SendCaptured(s.Runtime);

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, Order)), ",101,202,203,701,702,703");
    }

    Y_UNIT_TEST(UndeliveryOnFinishedRead) {
        TPortManager pm;
        Tests::TServerSettings serverSettings(pm.GetPort(2134));
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQueryStreamLookup(false);
        appConfig.MutableTableServiceConfig()->SetEnablePredicateExtractForScanQueries(true);
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(appConfig);

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);

        server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_DEBUG);
        server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
        TTestSetup s(ETestActorType::SorceRead, "/Root/Test", server.Get());

        NThreading::TPromise<bool> captured = NThreading::NewPromise<bool>();
        TVector<THolder<IEventHandle>> evts;
        std::atomic<bool> captureNotify = true;
        s.Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntimeBase::EEventAction {
                if (!captureNotify.load()) {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
                switch (ev->GetTypeRewrite()) {
                    case NYql::NDq::IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived::EventType: {
                        Cerr << "captured newasyncdataarrived" << Endl;
                        evts.push_back(THolder<IEventHandle>(ev.Release()));
                        if (!captured.HasValue()) {
                            captured.SetValue(true);
                        }
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });

        ExecSQL(*server->GetRuntime(), s.Sender, R"(
            CREATE TABLE `/Root/Test` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            )",
            false);

        ExecSQL(*server->GetRuntime(), s.Sender, R"(
            REPLACE INTO `/Root/Test` (Key, Value) VALUES
                (201u, "Value1"),
                (202u, "Value2"),
                (203u, "Value3"),
                (803u, "Value3");
            )",
            true);

        auto shards = s.Shards();
        UNIT_ASSERT_EQUAL(shards.size(), 1);

        s.SendScanQuery("SELECT Key FROM `/Root/Test` where Key = 202");

        s.Runtime->WaitFuture(captured.GetFuture());

        for (auto& ev : evts) {
            auto undelivery = MakeHolder<TEvPipeCache::TEvDeliveryProblem>(shards[0], true);

            s.Runtime->Send(ev->Sender, s.Sender, undelivery.Release());
        }

        captureNotify.store(false);

        for (auto& ev : evts) {
            s.Runtime->Send(ev.Release());
        }

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(s.CollectedKeys), ",202");
    }

    Y_UNIT_TEST(StreamLookupSplitBeforeReading) {
        TTestSetup s(ETestActorType::StreamLookup, "/Root/TestIndex");

        ExecSQL(*s.Runtime, s.Sender, R"(
            CREATE TABLE `/Root/TestIndex` (
                Key Uint64,
                Fk Uint64,
                Value String,
                PRIMARY KEY (Key),
                INDEX Index GLOBAL ON (Fk)
            );
        )", false);

        ExecSQL(*s.Runtime, s.Sender, R"(
            REPLACE INTO `/Root/TestIndex` (Key, Fk, Value) VALUES
                (1u, 10u, "Value1"),
                (2u, 10u, "Value2"),
                (3u, 10u, "Value3"),
                (4u, 11u, "Value4"),
                (5u, 12u, "Value5");
        )", true);

        auto shards = s.Shards();
        auto* shim = new TReadActorPipeCacheStub();

        InterceptStreamLookupActorPipeCache(s.Runtime->Register(shim));
        shim->SetupCapture(0, 1);
        s.SendScanQuery(
            "SELECT Key, Value FROM `/Root/TestIndex` VIEW Index where Fk in (10, 11) ORDER BY Key"
        );

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        s.Split(shards.at(0), 3);
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->SendCaptured(s.Runtime);

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, SortOrder::Ascending)), ",1,2,3,4");
    }

    Y_UNIT_TEST(StreamLookupSplitAfterFirstResult) {
        TTestSetup s(ETestActorType::StreamLookup, "/Root/TestIndex");

        ExecSQL(*s.Runtime, s.Sender, R"(
            CREATE TABLE `/Root/TestIndex` (
                Key Uint64,
                Fk Uint64,
                Value String,
                PRIMARY KEY (Key),
                INDEX Index GLOBAL ON (Fk)
            );
        )", false);

        ExecSQL(*s.Runtime, s.Sender, R"(
            REPLACE INTO `/Root/TestIndex` (Key, Fk, Value) VALUES
                (1u, 10u, "Value1"),
                (2u, 10u, "Value2"),
                (3u, 10u, "Value3"),
                (4u, 11u, "Value4"),
                (5u, 12u, "Value5");
        )", true);

        auto shards = s.Shards();

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(2);
        evread.SetMaxRows(2);
        SetDefaultReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        evreadack.SetMaxRows(2);
        SetDefaultReadAckSettings(evreadack);

        auto* shim = new TReadActorPipeCacheStub();
        shim->SetupCapture(1, 1);
        shim->SetupResultsCapture(1);
        InterceptStreamLookupActorPipeCache(s.Runtime->Register(shim));
        s.SendScanQuery(
            "SELECT Key, Value FROM `/Root/TestIndex` VIEW Index where Fk in (10, 11) ORDER BY Key"
        );

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        s.Split(shards.at(0), 3);
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->AllowResults();
        shim->SendCaptured(s.Runtime);

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, SortOrder::Ascending)), ",1,2,3,4");
    }

    Y_UNIT_TEST(StreamLookupRetryAttemptForFinishedRead) {
        TTestSetup s(ETestActorType::StreamLookup, "/Root/TestIndex");

        auto settings = MakeIntrusive<TIteratorReadBackoffSettings>();
        settings->StartRetryDelay = TDuration::MilliSeconds(250);
        settings->MaxShardAttempts = 4;
        // set small read response timeout (for frequent retries)
        settings->ReadResponseTimeout = TDuration::MilliSeconds(1);
        SetReadIteratorBackoffSettings(settings);

        ExecSQL(*s.Runtime, s.Sender, R"(
            CREATE TABLE `/Root/TestIndex` (
                Key Uint64,
                Fk Uint64,
                Value String,
                PRIMARY KEY (Key),
                INDEX Index GLOBAL ON (Fk)
            );
        )", false);

        ExecSQL(*s.Runtime, s.Sender, R"(
            REPLACE INTO `/Root/TestIndex` (Key, Fk, Value) VALUES
                (1u, 10u, "Value1"),
                (2u, 10u, "Value2"),
                (3u, 10u, "Value3"),
                (4u, 11u, "Value4"),
                (5u, 12u, "Value5");
        )", true);

        auto shards = s.Shards();
        auto* shim = new TReadActorPipeCacheStub();

        InterceptStreamLookupActorPipeCache(s.Runtime->Register(shim));
        // capture first evread, retry attempt for this read was scheduled
        shim->SetupCapture(0, 1);

        s.SendScanQuery(
            "SELECT Key, Value FROM `/Root/TestIndex` VIEW Index where Fk in (10, 11) ORDER BY Key"
        );

        shim->ReadsReceived.WaitI();
        
        UNIT_ASSERT_EQUAL(shards.size(), 1);
        auto undelivery = MakeHolder<TEvPipeCache::TEvDeliveryProblem>(shards[0], true);

        UNIT_ASSERT_EQUAL(shim->Captured.size(), 1);
        // send delivery problem, read should be restarted (it will be second retry attempt for this read)
        s.Runtime->Send(shim->Captured[0]->Sender, s.Sender, undelivery.Release());

        shim->SkipAll();

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, SortOrder::Ascending)), ",1,2,3,4");
    }

    Y_UNIT_TEST(StreamLookupDeliveryProblem) {
        TTestSetup s(ETestActorType::StreamLookup, "/Root/TestIndex");

        ExecSQL(*s.Runtime, s.Sender, R"(
            CREATE TABLE `/Root/TestIndex` (
                Key Uint64,
                Fk Uint64,
                Value String,
                PRIMARY KEY (Key),
                INDEX Index GLOBAL ON (Fk)
            );
        )", false);

        ExecSQL(*s.Runtime, s.Sender, R"(
            REPLACE INTO `/Root/TestIndex` (Key, Fk, Value) VALUES
                (1u, 10u, "Value1"),
                (2u, 10u, "Value2"),
                (3u, 10u, "Value3"),
                (4u, 11u, "Value4"),
                (5u, 12u, "Value5");
        )", true);

        auto shards = s.Shards();
        auto* shim = new TReadActorPipeCacheStub();

        InterceptStreamLookupActorPipeCache(s.Runtime->Register(shim));
        shim->SetupCapture(0, 1);

        s.SendScanQuery(
            "SELECT Key, Value FROM `/Root/TestIndex` VIEW Index where Fk in (10, 11) ORDER BY Key"
        );

        shim->ReadsReceived.WaitI();
        
        UNIT_ASSERT_EQUAL(shards.size(), 1);
        auto undelivery = MakeHolder<TEvPipeCache::TEvDeliveryProblem>(shards[0], true);

        UNIT_ASSERT_EQUAL(shim->Captured.size(), 1);
        s.Runtime->Send(shim->Captured[0]->Sender, s.Sender, undelivery.Release());

        shim->SkipAll();
        shim->SendCaptured(s.Runtime);

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, SortOrder::Ascending)), ",1,2,3,4");

    }

    // TODO: rework test for stream lookups
    //Y_UNIT_TEST_SORT(AfterResolvePoints, Order) {
    //    TTestSetup s;
    //    auto shards = s.Shards();

    //    auto* shim = new TReadActorPipeCacheStub();
    //    InterceptReadActorPipeCache(s.Runtime->Register(shim));
    //    shim->SetupCapture(0, 5);
    //    s.SendScanQuery(
    //        "PRAGMA Kikimr.OptEnablePredicateExtract=\"false\"; SELECT Key FROM `/Root/KeyValueLargePartition` where Key in (103, 302, 402, 502, 703)" + OrderBy(Order));

    //    shim->ReadsReceived.WaitI();
    //    Cerr << "starting split -----------------------------------------------------------" << Endl;
    //    s.Split(shards.at(0), 400);
    //    Cerr << "resume evread -----------------------------------------------------------" << Endl;
    //    shim->SkipAll();
    //    shim->SendCaptured(s.Runtime);

    //    s.AssertSuccess();
    //    UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, Order)), ",103,302,402,502,703");
    //}
}


} // namespace NKqp
} // namespace NKikimr
