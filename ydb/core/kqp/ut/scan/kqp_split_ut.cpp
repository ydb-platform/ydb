#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/datashard_impl.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/runtime/kqp_stream_lookup_actor.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <util/generic/size_literals.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/testlib/actors/block_events.h>

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

    void SetSplitMergePartCountLimit(TTestActorRuntime* runtime, i64 val) {
        TControlBoard::SetValue(val, runtime->GetAppData().Icb->SchemeShardControls.SplitMergePartCountLimit);
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
        TActorId PipeCache = MakePipePerNodeCacheID(false);
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
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetYdbStatus(), code);
    }

    void SendDataQuery(TTestActorRuntime* runtime, TActorId kqpProxy, TActorId sender, const TString& queryText) {
        auto ev = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
        ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        ev->Record.MutableRequest()->SetQuery(queryText);
        ev->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
        ActorIdToProto(sender, ev->Record.MutableRequestActorId());
        runtime->Send(new IEventHandle(kqpProxy, sender, ev.release()));
    }

    void SendScanQuery(TTestActorRuntime* runtime, TActorId kqpProxy, TActorId sender, const TString& queryText) {
        auto ev = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCAN);
        ev->Record.MutableRequest()->SetQuery(queryText);
        ev->Record.MutableRequest()->SetKeepSession(false);
        ActorIdToProto(sender, ev->Record.MutableRequestActorId());
        runtime->Send(new IEventHandle(kqpProxy, sender, ev.release()));
    }

    void CollectKeysTo(TVector<ui64>* collectedKeys, TTestActorRuntime* runtime, TActorId sender) {
        auto captureEvents = [=](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKqp::TEvKqpExecuter::TEvStreamData::EventType) {
                auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;
                for (auto& row : record.resultset().rows()) {
                    collectedKeys->push_back(row.items(0).uint64_value());
                }

                auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                resp->Record.SetEnough(false);
                runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                return true;
            }
            if (ev->GetTypeRewrite() == NKqp::TEvKqp::TEvQueryResponse::EventType) {
                auto& record = ev->Get<NKqp::TEvKqp::TEvQueryResponse>()->Record;
                for (auto& resultSet : record.GetResponse().GetYdbResults()) {
                    for (auto& row : resultSet.rows()) {
                        collectedKeys->push_back(row.items(0).uint64_value());
                    }
                }
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
                InterceptReadActorPipeCache(MakePipePerNodeCacheID(false));
            } else if (testActorType == ETestActorType::StreamLookup) {
                InterceptStreamLookupActorPipeCache(MakePipePerNodeCacheID(false));
            }

            if (providedServer) {
                Server = providedServer;
            } else {
                TKikimrSettings settings;
                settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(testActorType == ETestActorType::SorceRead);
                settings.AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(true);
                settings.SetDomainRoot(KikimrDefaultUtDomainRoot);

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
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        }

        void SendScanQuery(TString text) {
           ::NKikimr::NKqp::NTestSuiteKqpSplit::SendScanQuery(Runtime, KqpProxy, Sender, text);
        }

        void SendDataQuery(TString text) {
           ::NKikimr::NKqp::NTestSuiteKqpSplit::SendDataQuery(Runtime, KqpProxy, Sender, text);
        }

        TMaybe<TKikimrRunner> Kikimr;
        TVector<ui64> CollectedKeys;
        Tests::TServer* Server;
        NActors::TTestActorRuntime* Runtime;
        TActorId KqpProxy;
        TActorId Sender;

        TString Table;
    };

    Tests::TServer::TPtr MakeSamplingServer() {
        TPortManager ports;
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        config.MutableTableServiceConfig()->MutableAggregationConfig()->SetDSScanMinimalThreads(2);
        auto settings = Tests::TServerSettings(ports.GetPort(2134))
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(config);
        return new Tests::TServer(settings);
    }

    void CreateSamplingTable(TTestSetup& setup, ui32 shards = 1) {
        TStringBuilder create;
        create << "CREATE TABLE `/Root/Sampling` (Key Uint64, Value String, PRIMARY KEY (Key))";
        if (shards > 1) {
            create << " WITH (PARTITION_AT_KEYS = (";
            for (ui32 shard = 1; shard < shards; ++shard) {
                if (shard > 1) {
                    create << ", ";
                }
                create << shard * 10;
            }
            create << "))";
        }
        ExecSQL(*setup.Runtime, setup.Sender, create, false);
        TStringBuilder write;
        write << "UPSERT INTO `/Root/Sampling` (Key, Value) VALUES ";
        for (ui32 key = 0; key < shards * 10; ++key) {
            if (key) {
                write << ", ";
            }
            write << "(" << key << "u, \"value\")";
        }
        ExecSQL(*setup.Runtime, setup.Sender, write);
    }

    TString SamplingQuery(TStringBuf rate = "1", TStringBuf stride = "1000000") {
        return TStringBuilder() << "SELECT Key FROM `/Root/Sampling` WITH (sampling_rate=\""
            << rate << "\", sampling_seed=\"42\", sampling_memtable_stride=\"" << stride << "\")";
    }

    void SetSamplingQuota(ui32 rows) {
        NKikimrTxDataShard::TEvRead read;
        read.SetMaxRowsInResult(rows);
        read.SetMaxRows(rows);
        SetDefaultReadSettings(read);
        NKikimrTxDataShard::TEvReadAck ack;
        ack.SetMaxRows(rows);
        SetDefaultReadAckSettings(ack);
    }

    void AssertSamplingKeys(const TTestSetup& setup, ui32 rows, ui32 first = 0) {
        auto keys = Canonize(setup.CollectedKeys, SortOrder::Unspecified);
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), rows);
        for (ui32 i = 0; i < rows; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(keys[i], first + i);
        }
    }

    Y_UNIT_TEST_TWIN(SamplingHintReachesReadAndStreamsAcrossAcks, batchHint) {
        auto server = MakeSamplingServer();
        TTestSetup setup(ETestActorType::SorceRead, "/Root/Sampling", server.Get());
        CreateSamplingTable(setup);
        SetSamplingQuota(2);
        ui32 reads = 0;
        ui32 acks = 0;
        ui32 partialResults = 0;
        auto observeRead = setup.Runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (batchHint) {
                record.SetHints(record.GetHints() | TEvDataShard::TEvRead::HINT_BATCH);
            }
            UNIT_ASSERT(record.HasSampling());
            UNIT_ASSERT_VALUES_EQUAL(record.GetSampling().GetRate(), 1.0);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSampling().GetSeed(), 42u);
            UNIT_ASSERT_VALUES_EQUAL(record.GetSampling().GetMemtableStride(), 1000000u);
            UNIT_ASSERT(!record.GetSampling().HasContinuation());
            ++reads;
        });
        auto observeAck = setup.Runtime->AddObserver<TEvDataShard::TEvReadAck>([&](auto&) {
            ++acks;
        });
        auto observeResult = setup.Runtime->AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (!record.GetFinished() && record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
                NKikimrTxDataShard::TReadContinuationToken token;
                UNIT_ASSERT(token.ParseFromString(record.GetContinuationToken()));
                UNIT_ASSERT(token.HasSampling());
                UNIT_ASSERT(token.GetSampling().HasPendingSelectedUnit());
                UNIT_ASSERT(!token.GetSampling().GetLastProcessedKeyInclusive());
                ++partialResults;
            }
        });
        setup.SendScanQuery(SamplingQuery());
        setup.AssertSuccess();
        AssertSamplingKeys(setup, 10);
        UNIT_ASSERT_VALUES_EQUAL(reads, 1u);
        UNIT_ASSERT(acks > 0);
        UNIT_ASSERT(partialResults > 0);
    }

    Y_UNIT_TEST(SamplingSelectedUnitSurvivesCompactionAfterPartialOutput) {
        auto server = MakeSamplingServer();
        TTestSetup setup(ETestActorType::SorceRead, "/Root/Sampling", server.Get());
        CreateSamplingTable(setup);
        SetSamplingQuota(2);
        TBlockEvents<TEvDataShard::TEvReadAck> blockedAcks(*setup.Runtime);
        ui32 partialRows = 0;
        ui64 unitsSelected = 0;
        auto results = setup.Runtime->AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            if (ev->Get()->Record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
                partialRows += ev->Get()->GetRowsCount();
                unitsSelected = ev->Get()->Record.GetSamplingStats().GetUnitsSelected();
            }
        });
        setup.SendScanQuery(SamplingQuery());
        setup.Runtime->WaitFor("sampling reader exhausted its quota", [&] { return !blockedAcks.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(partialRows, 2u);
        UNIT_ASSERT_VALUES_EQUAL(unitsSelected, 1u);
        WaitForCompaction(server.Get(), setup.Table);
        blockedAcks.Stop().Unblock();
        setup.AssertSuccess();
        AssertSamplingKeys(setup, 10);
        // Compaction must not cause a second draw inside the retained selected unit.
        UNIT_ASSERT_VALUES_EQUAL(unitsSelected, 1u);
    }

    Y_UNIT_TEST_TWIN(SamplingSelectedUnitSurvivesSplitAfterPartialOutput, multipleRanges) {
        auto server = MakeSamplingServer();
        TTestSetup setup(ETestActorType::SorceRead, "/Root/Sampling", server.Get());
        CreateSamplingTable(setup);
        auto shards = setup.Shards();
        SetSamplingQuota(2);
        TBlockEvents<TEvDataShard::TEvReadAck> blockedAcks(*setup.Runtime);
        THashMap<TActorId, THashSet<ui64>> resumedReaders;
        ui32 resumedReads = 0;
        ui32 independentRanges = 0;
        auto reads = setup.Runtime->AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            const auto* forward = ev->Get();
            if (forward->Ev->Type() != TEvDataShard::TEvRead::EventType || forward->TabletId == shards.at(0)) {
                return;
            }
            const auto* read = static_cast<const TEvDataShard::TEvRead*>(forward->Ev.Get());
            const auto& sampling = read->Record.GetSampling();
            UNIT_ASSERT_VALUES_EQUAL(read->Ranges.size(), 1u);
            const auto& range = read->Ranges.front();
            if (sampling.GetContinuation().HasPendingSelectedUnit()) {
                ++resumedReads;
                resumedReaders[ev->Sender].insert(read->Record.GetReadId());
                UNIT_ASSERT_VALUES_EQUAL(sampling.GetContinuation().GetLastProcessedKeyInclusive(), range.FromInclusive);
            } else {
                UNIT_ASSERT(multipleRanges);
                UNIT_ASSERT_VALUES_EQUAL(range.From.GetCells().front().AsValue<ui64>(), 6u);
                ++independentRanges;
            }
        });
        auto results = setup.Runtime->AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            const auto reader = resumedReaders.find(ev->GetRecipientRewrite());
            if (record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS
                && reader != resumedReaders.end() && reader->second.contains(record.GetReadId()))
            {
                UNIT_ASSERT_VALUES_EQUAL(record.GetSamplingStats().GetUnitsTotal(), 0u);
            }
        });
        setup.SendScanQuery(SamplingQuery() + (multipleRanges ? " WHERE Key < 4u OR Key >= 6u" : ""));
        setup.Runtime->WaitFor("sampling reader exhausted its quota", [&] { return !blockedAcks.empty(); });
        setup.Split(shards.at(0), multipleRanges ? 5 : 3);
        blockedAcks.Stop().Unblock();
        setup.AssertSuccess();
        if (multipleRanges) {
            UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(setup.CollectedKeys, SortOrder::Unspecified)), ",0,1,2,3,6,7,8,9");
            UNIT_ASSERT_VALUES_EQUAL(independentRanges, 1u);
        } else {
            AssertSamplingKeys(setup, 10);
        }
        UNIT_ASSERT_VALUES_EQUAL(resumedReads, multipleRanges ? 1u : 2u);
    }

    Y_UNIT_TEST(SamplingSelectedUnitSurvivesCompositeKeyPrefixSplit) {
        auto server = MakeSamplingServer();
        TTestSetup setup(ETestActorType::SorceRead, "/Root/Sampling", server.Get());
        ExecSQL(*setup.Runtime, setup.Sender, R"(
            CREATE TABLE `/Root/Sampling` (
                Key Uint64, Subkey Uint64, Value Uint64, PRIMARY KEY (Key, Subkey)
            ) WITH (PARTITION_AT_KEYS = ((10)))
        )", false);
        TStringBuilder write;
        write << "UPSERT INTO `/Root/Sampling` (Key, Subkey, Value) VALUES ";
        for (ui32 value = 0; value < 36; ++value) {
            if (value) {
                write << ", ";
            }
            write << "(" << value / 3 << "u, ";
            if (value % 3) {
                write << value % 3 << "u";
            } else {
                write << "NULL";
            }
            write << ", " << value << "u)";
        }
        ExecSQL(*setup.Runtime, setup.Sender, write);
        const auto shards = setup.Shards();
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        SetSamplingQuota(2);
        TBlockEvents<TEvDataShard::TEvReadAck> blockedAcks(*setup.Runtime);
        THashMap<TActorId, THashSet<ui64>> resumedReaders;
        ui32 resumedReads = 0;
        auto reads = setup.Runtime->AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            const auto* forward = ev->Get();
            if (forward->Ev->Type() != TEvDataShard::TEvRead::EventType
                || std::find(shards.begin(), shards.end(), forward->TabletId) != shards.end())
            {
                return;
            }
            const auto* read = static_cast<const TEvDataShard::TEvRead*>(forward->Ev.Get());
            const auto& continuation = read->Record.GetSampling().GetContinuation();
            UNIT_ASSERT(continuation.HasPendingSelectedUnit());
            UNIT_ASSERT_VALUES_EQUAL(read->Ranges.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(continuation.GetLastProcessedKeyInclusive(), read->Ranges.front().FromInclusive);
            resumedReaders[ev->Sender].insert(read->Record.GetReadId());
            ++resumedReads;
        });
        ui32 resumedRows = 0;
        auto results = setup.Runtime->AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            const auto reader = resumedReaders.find(ev->GetRecipientRewrite());
            if (record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS
                && reader != resumedReaders.end() && reader->second.contains(record.GetReadId()))
            {
                // Both children must retain the parent's decision through all ACKs.
                UNIT_ASSERT_VALUES_EQUAL(record.GetSamplingStats().GetUnitsTotal(), 0u);
                resumedRows += ev->Get()->GetRowsCount();
            }
        });
        setup.SendScanQuery(R"(
            SELECT Value FROM `/Root/Sampling`
            WITH (sampling_rate="1", sampling_seed="42", sampling_memtable_stride="1000000")
        )");
        setup.Runtime->WaitFor("both sampling readers exhausted their quotas", [&] { return blockedAcks.size() == 2; });
        setup.Split(shards.at(0), 5);
        blockedAcks.Stop().Unblock();
        setup.AssertSuccess();
        AssertSamplingKeys(setup, 36);
        UNIT_ASSERT_VALUES_EQUAL(resumedReads, 2u);
        UNIT_ASSERT_VALUES_EQUAL(resumedRows, 28u);
    }

    Y_UNIT_TEST(SamplingRejectsParameterizedLookup) {
        auto server = MakeSamplingServer();
        TTestSetup setup(ETestActorType::SorceRead, "/Root/Sampling", server.Get());
        CreateSamplingTable(setup);
        ui32 reads = 0;
        auto requests = setup.Runtime->AddObserver<TEvDataShard::TEvRead>([&](auto&) {
            ++reads;
        });

        auto ev = std::make_unique<TEvKqp::TEvQueryRequest>();
        auto& request = *ev->Record.MutableRequest();
        request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request.SetType(NKikimrKqp::QUERY_TYPE_SQL_SCAN);
        request.SetKeepSession(false);
        auto& parameter = (*request.MutableYdbParameters())["$key"];
        request.SetQuery("DECLARE $key AS Uint64?; " + SamplingQuery() + " WHERE Key = $key");
        parameter.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        parameter.mutable_value()->set_uint64_value(1);
        ActorIdToProto(setup.Sender, ev->Record.MutableRequestActorId());
        setup.Runtime->Send(new IEventHandle(setup.KqpProxy, setup.Sender, ev.release()));

        auto reply = setup.Runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(setup.Sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(reply->Get()->Record.DebugString(), "Sampling does not support key lookups");
        UNIT_ASSERT(setup.CollectedKeys.empty());
        UNIT_ASSERT_VALUES_EQUAL(reads, 0u);
    }

    Y_UNIT_TEST(SamplingRejectsUnsampledFinishedResult) {
        auto server = MakeSamplingServer();
        TTestSetup setup(ETestActorType::SorceRead, "/Root/Sampling", server.Get());
        CreateSamplingTable(setup);
        SetSamplingQuota(100);
        ui32 reads = 0;
        auto requests = setup.Runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            UNIT_ASSERT(record.HasSampling());
            // Model an older DataShard that ignores the unknown Sampling field.
            record.ClearSampling();
            ++reads;
        });
        ui32 finishedResults = 0;
        auto results = setup.Runtime->AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
                UNIT_ASSERT(record.GetFinished());
                UNIT_ASSERT(!record.HasSamplingStats());
                UNIT_ASSERT(!record.HasContinuationToken());
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetRowsCount(), 10u);
                ++finishedResults;
            }
        });
        setup.SendScanQuery(SamplingQuery("0.00000000000000000001"));
        auto reply = setup.Runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(setup.Sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::ABORTED);
        UNIT_ASSERT_STRING_CONTAINS(reply->Get()->Record.DebugString(), "DataShard did not confirm sampling support");
        UNIT_ASSERT(setup.CollectedKeys.empty());
        UNIT_ASSERT_VALUES_EQUAL(reads, 1u);
        UNIT_ASSERT_VALUES_EQUAL(finishedResults, 1u);
    }

    Y_UNIT_TEST(SamplingSchemaErrorInvalidatesQueryWithoutRetry) {
        auto server = MakeSamplingServer();
        TTestSetup setup(ETestActorType::SorceRead, "/Root/Sampling", server.Get());
        CreateSamplingTable(setup);
        SetSamplingQuota(100);
        ui32 reads = 0;
        auto requests = setup.Runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            UNIT_ASSERT(record.HasSampling());
            // Force the same rejection as a cached plan with an outdated schema.
            auto* table = record.MutableTableId();
            table->SetSchemaVersion(table->GetSchemaVersion() + 1);
            ++reads;
        });
        ui32 schemaErrors = 0;
        auto results = setup.Runtime->AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(), Ydb::StatusIds::SCHEME_ERROR);
            UNIT_ASSERT(!record.HasContinuationToken());
            ++schemaErrors;
        });
        setup.SendScanQuery(SamplingQuery());
        auto reply = setup.Runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(setup.Sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::ABORTED);
        NYql::TIssues issues;
        NYql::IssuesFromMessage(reply->Get()->Record.GetResponse().GetQueryIssues(), issues);
        UNIT_ASSERT_C(HasIssue(issues, NYql::TIssuesIds::KIKIMR_QUERY_INVALIDATED), issues.ToString());
        UNIT_ASSERT(setup.CollectedKeys.empty());
        UNIT_ASSERT_VALUES_EQUAL(reads, 1u);
        UNIT_ASSERT_VALUES_EQUAL(schemaErrors, 1u);
    }

    Y_UNIT_TEST_TWIN(SamplingSelectedUnitSurvivesOverloadedRetry, inclusiveCursor) {
        auto server = MakeSamplingServer();
        TTestSetup setup(ETestActorType::SorceRead, "/Root/Sampling", server.Get());
        CreateSamplingTable(setup);
        SetSamplingQuota(2);
        TBlockEvents<TEvDataShard::TEvReadAck> blockedAcks(*setup.Runtime);
        NKikimrTxDataShard::TReadContinuationToken token;
        TActorId reader;
        TActorId shard;
        ui64 initialReadId = 0;
        ui64 lastSeqNo = 0;
        ui32 reads = 0;
        auto requests = setup.Runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            const auto& read = *ev->Get();
            if (reads++ == 0) {
                reader = ev->Sender;
                shard = ev->GetRecipientRewrite();
                initialReadId = read.Record.GetReadId();
            } else {
                UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite(), shard);
                UNIT_ASSERT_VALUES_EQUAL(read.Ranges.size(), 1u);
                UNIT_ASSERT_VALUES_EQUAL(read.Ranges.front().From.GetBuffer(), token.GetLastProcessedKey());
                UNIT_ASSERT_VALUES_EQUAL(read.Ranges.front().FromInclusive, inclusiveCursor);
                UNIT_ASSERT_VALUES_EQUAL(read.Record.GetSampling().GetContinuation().SerializeAsString(),
                    token.GetSampling().SerializeAsString());
            }
        });
        ui32 partialRows = 0;
        ui32 resumedRows = 0;
        auto results = setup.Runtime->AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
                return;
            }
            if (record.GetReadId() == initialReadId) {
                UNIT_ASSERT(token.ParseFromString(record.GetContinuationToken()));
                UNIT_ASSERT(token.GetSampling().HasPendingSelectedUnit());
                lastSeqNo = record.GetSeqNo();
                partialRows += ev->Get()->GetRowsCount();
            } else {
                UNIT_ASSERT_VALUES_EQUAL(record.GetSamplingStats().GetUnitsTotal(), 0u);
                resumedRows += ev->Get()->GetRowsCount();
            }
        });
        setup.SendScanQuery(SamplingQuery());
        setup.Runtime->WaitFor("sampling reader exhausted its quota", [&] { return !blockedAcks.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(partialRows, 2u);
        if (inclusiveCursor) {
            // Before(2) resumes the same unread rows as the original After(1).
            token.SetLastProcessedKey(TSerializedCellVec::Serialize({TCell::Make(ui64(2))}));
            token.MutableSampling()->SetLastProcessedKeyInclusive(true);
        }
        auto stopped = MakeHolder<TEvDataShard::TEvReadResult>();
        stopped->Record.SetReadId(initialReadId);
        stopped->Record.SetSeqNo(lastSeqNo + 1);
        stopped->Record.MutableStatus()->SetCode(Ydb::StatusIds::OVERLOADED);
        stopped->Record.SetThrottleDelayMs(0);
        stopped->Record.SetContinuationToken(token.SerializeAsString());
        // The exhausted reader cannot advance while its ACK is held. The retry
        // cancels it; discard that ACK and allow ACKs for the replacement reader.
        setup.Runtime->Send(new IEventHandle(reader, shard, stopped.Release()));
        blockedAcks.Stop();
        setup.AssertSuccess();
        AssertSamplingKeys(setup, 10);
        UNIT_ASSERT_VALUES_EQUAL(reads, 2u);
        UNIT_ASSERT_VALUES_EQUAL(resumedRows, 8u);
    }

    Y_UNIT_TEST_TWIN(SamplingLostReaderFailsWithoutRetry, beforeFirstResult) {
        auto server = MakeSamplingServer();
        TTestSetup setup(ETestActorType::SorceRead, "/Root/Sampling", server.Get());
        CreateSamplingTable(setup);
        const auto shards = setup.Shards();
        SetSamplingQuota(2);
        TBlockEvents<TEvDataShard::TEvReadAck> blockedAcks(*setup.Runtime);
        ui32 reads = 0;
        TActorId reader;
        auto observe = setup.Runtime->AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            if (ev->Get()->Ev->Type() == TEvDataShard::TEvRead::EventType) {
                ++reads;
                reader = ev->Sender;
            }
        });
        TBlockEvents<TEvDataShard::TEvRead> blockedReads(*setup.Runtime, [&](const auto&) { return beforeFirstResult; });
        setup.SendScanQuery(SamplingQuery());
        setup.Runtime->WaitFor("sampling reader before losing its state", [&] {
            return beforeFirstResult ? !blockedReads.empty() : !blockedAcks.empty();
        });
        setup.Runtime->Send(reader, setup.Sender,
            new TEvPipeCache::TEvDeliveryProblem(shards.at(0), true));
        auto reply = setup.Runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(setup.Sender);
        UNIT_ASSERT_C(reply->Get()->Record.GetYdbStatus() != Ydb::StatusIds::SUCCESS,
            reply->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(reads, 1u);
    }

    Y_UNIT_TEST(SamplingEmptyResultPreservesInclusiveCursorAfterSplit) {
        auto server = MakeSamplingServer();
        TTestSetup setup(ETestActorType::SorceRead, "/Root/Sampling", server.Get());
        CreateSamplingTable(setup);
        const auto shards = setup.Shards();
        SetSamplingQuota(2);
        TBlockEvents<TEvDataShard::TEvReadAck> blockedAcks(*setup.Runtime);
        TEvDataShard::TEvRead::TPtr initialRead;
        NKikimrTxDataShard::TReadContinuationToken token;
        const ui64 boundary = 5;
        token.SetLastProcessedKey(TSerializedCellVec::Serialize({TCell::Make(boundary)}));
        token.SetFirstUnprocessedQuery(0);
        token.MutableSampling()->SetLastProcessedKeyInclusive(true);
        ui32 resumedReads = 0;
        auto observe = setup.Runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            if (!initialRead) {
                // Model a quota yield after skipping [start, 5): the cursor is
                // inclusive even though there are no rows in this result.
                auto reply = MakeHolder<TEvDataShard::TEvReadResult>();
                auto& record = reply->Record;
                record.SetReadId(ev->Get()->Record.GetReadId());
                record.SetSeqNo(1);
                record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
                record.SetRowCount(0);
                record.SetLimitReached(true);
                record.SetResultFormat(ev->Get()->Record.GetResultFormat());
                *record.MutableSnapshot() = ev->Get()->Record.GetSnapshot();
                record.MutableSamplingStats();
                record.SetContinuationToken(token.SerializeAsString());
                setup.Runtime->Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), reply.Release()));
                initialRead = std::move(ev);
            } else {
                UNIT_ASSERT(ev->Get()->Record.GetSampling().HasContinuation());
                UNIT_ASSERT(!ev->Get()->Record.GetSampling().GetContinuation().HasPendingSelectedUnit());
                if (resumedReads++ == 0) {
                    const auto& range = ev->Get()->Ranges.front();
                    UNIT_ASSERT_VALUES_EQUAL(range.From.GetBuffer(), token.GetLastProcessedKey());
                    UNIT_ASSERT(range.FromInclusive);
                }
            }
        });
        setup.SendScanQuery(SamplingQuery());
        setup.Runtime->WaitFor("ACK for the empty sampling result", [&] { return !blockedAcks.empty(); });
        UNIT_ASSERT(setup.CollectedKeys.empty());
        setup.Split(shards.at(0), 7);

        // The old reader is stopped and hands over the same progress atomically.
        // A successful result alone cannot authorize recreating the reader.
        auto stopped = MakeHolder<TEvDataShard::TEvReadResult>();
        stopped->Record.SetReadId(initialRead->Get()->Record.GetReadId());
        stopped->Record.SetSeqNo(2);
        stopped->Record.MutableStatus()->SetCode(Ydb::StatusIds::NOT_FOUND);
        stopped->Record.SetContinuationToken(token.SerializeAsString());
        setup.Runtime->Send(new IEventHandle(initialRead->Sender, initialRead->GetRecipientRewrite(), stopped.Release()));
        blockedAcks.Stop();
        setup.AssertSuccess();
        AssertSamplingKeys(setup, 5, 5);
        UNIT_ASSERT_VALUES_EQUAL(resumedReads, 2u);
    }

    Y_UNIT_TEST(SamplingSplitDoesNotExceedMaxInFlight) {
        auto server = MakeSamplingServer();
        TTestSetup setup(ETestActorType::SorceRead, "/Root/Sampling", server.Get());
        CreateSamplingTable(setup, 16);
        const auto shards = setup.Shards();
        SetSamplingQuota(2);
        TBlockEvents<TEvDataShard::TEvReadAck> blockedAcks(*setup.Runtime);
        THashMap<TActorId, THashMap<ui64, ui64>> readers;
        THashSet<TActorId> sourceActors;
        ui32 inFlight = 0;
        ui32 maxInFlight = 0;
        ui64 shardToSplit = 0;
        auto requests = setup.Runtime->AddObserver<TEvPipeCache::TEvForward>([&](auto& ev) {
            auto* forward = ev->Get();
            if (forward->Ev->Type() != TEvDataShard::TEvRead::EventType) {
                return;
            }
            const auto* read = static_cast<TEvDataShard::TEvRead*>(forward->Ev.Get());
            if (!read->Record.HasSampling()) {
                return;
            }
            UNIT_ASSERT(readers[ev->Sender].emplace(read->Record.GetReadId(), forward->TabletId).second);
            sourceActors.insert(ev->Sender);
            maxInFlight = Max(maxInFlight, ++inFlight);
            UNIT_ASSERT_C(inFlight <= 12, "A split exceeded the query's sampling shard budget");
            if (!shardToSplit) {
                shardToSplit = forward->TabletId;
            }
        });
        auto results = setup.Runtime->AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetFinished() || record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
                auto it = readers.find(ev->GetRecipientRewrite());
                if (it != readers.end() && it->second.erase(record.GetReadId())) {
                    --inFlight;
                }
            }
        });
        setup.SendScanQuery(SamplingQuery());
        setup.Runtime->WaitFor("all sampling shard slots occupied", [&] { return blockedAcks.size() == 12; });
        UNIT_ASSERT_VALUES_EQUAL(inFlight, 12u);
        UNIT_ASSERT(sourceActors.size() > 1);
        const auto shard = std::find(shards.begin(), shards.end(), shardToSplit);
        UNIT_ASSERT(shard != shards.end());
        setup.Split(shardToSplit, (shard - shards.begin()) * 10 + 5);
        blockedAcks.Stop().Unblock();
        setup.AssertSuccess();
        AssertSamplingKeys(setup, 160);
        UNIT_ASSERT_VALUES_EQUAL(maxInFlight, 12u);
        UNIT_ASSERT_VALUES_EQUAL(inFlight, 0u);
    }

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

    Y_UNIT_TEST(StreamLookupJoinSplitBeforeReading) {
        TTestSetup s(ETestActorType::StreamLookup, "/Root/Table1");

        ExecSQL(*s.Runtime, s.Sender, R"(
            --!syntax_v1
            CREATE TABLE `/Root/Table1` (Key uint64, Key2 uint64, Value uint64, PRIMARY KEY(Key, Key2));
        )", false);

        ExecSQL(*s.Runtime, s.Sender, R"(
            REPLACE INTO `/Root/Table1` (Key, Key2, Value) VALUES
                (1u, 1u, 1u),
                (1u, 2u, 2u),
                (1u, 3u, 3u),
                (2147483648u, 4u, 4u);
        )", true);

         auto shards = s.Shards();
        auto* shim = new TReadActorPipeCacheStub();

        InterceptStreamLookupActorPipeCache(s.Runtime->Register(shim));
        shim->SetupCapture(0, 1);
        s.SendDataQuery(R"(
            $data = AsList(
                AsStruct(1u AS Key, 1u AS Value),
                AsStruct(2147483648u AS Key, 2147483648u AS Value));

            SELECT b.Value
            FROM AS_TABLE($data) a
            JOIN `/Root/Table1` b
            ON a.Key = b.Key
            ORDER BY b.Value ASC;
        )");

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

    Y_UNIT_TEST(StreamLookupJoinSplitAfterFirstResult) {
        TTestSetup s(ETestActorType::StreamLookup, "/Root/Table1");

        ExecSQL(*s.Runtime, s.Sender, R"(
            --!syntax_v1
            CREATE TABLE `/Root/Table1` (Key uint64, Key2 uint64, Value uint64, PRIMARY KEY(Key, Key2));
        )", false);

        ExecSQL(*s.Runtime, s.Sender, R"(
            REPLACE INTO `/Root/Table1` (Key, Key2, Value) VALUES
                (1u, 1u, 1u),
                (1u, 2u, 2u),
                (1u, 3u, 3u),
                (2147483648u, 4u, 4u);
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
        s.SendDataQuery(R"(
            $data = AsList(
                AsStruct(1u AS Key, 1u AS Value),
                AsStruct(2147483648u AS Key, 2147483648u AS Value));

            SELECT b.Value
            FROM AS_TABLE($data) a
            JOIN `/Root/Table1` b
            ON a.Key = b.Key
            ORDER BY b.Value ASC;
        )");

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

    Y_UNIT_TEST(StreamLookupJoinDeliveryProblemAfterFirstResult) {
        TTestSetup s(ETestActorType::StreamLookup, "/Root/Table1");

        ExecSQL(*s.Runtime, s.Sender, R"(
            --!syntax_v1
            CREATE TABLE `/Root/Table1` (Key uint64, Key2 uint64, Value uint64, PRIMARY KEY(Key, Key2));
        )", false);

        ExecSQL(*s.Runtime, s.Sender, R"(
            REPLACE INTO `/Root/Table1` (Key, Key2, Value) VALUES
                (1u, 1u, 1u),
                (1u, 2u, 2u),
                (3u, 3u, 3u),
                (3u, 4u, 4u),
                (4u, 5u, 5u),
                (4u, 6u, 6u);
        )", true);

        auto shards = s.Shards();

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(3);
        evread.SetMaxRows(3);
        SetDefaultReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        evreadack.SetMaxRows(3);
        SetDefaultReadAckSettings(evreadack);

        auto* shim = new TReadActorPipeCacheStub();
        shim->SetupCapture(1, 1);
        shim->SetupResultsCapture(1);
        InterceptStreamLookupActorPipeCache(s.Runtime->Register(shim));
        s.SendDataQuery(R"(
            $data = AsList(
                AsStruct(1u AS Key, 1u AS Value),
                AsStruct(2u AS Key, 2u AS Value),
                AsStruct(3u AS Key, 3u AS Value),
                AsStruct(4u AS Key, 4u AS Value));

            SELECT b.Value
            FROM AS_TABLE($data) a
            LEFT JOIN `/Root/Table1` b
            ON a.Key = b.Key
            ORDER BY a.Key, b.Value ASC;
        )");

        shim->ReadsReceived.WaitI();
        Cerr << "delivery problem -----------------------------------------------------------" << Endl;
        UNIT_ASSERT_EQUAL(shards.size(), 1);
        auto undelivery = MakeHolder<TEvPipeCache::TEvDeliveryProblem>(shards[0], true);

        UNIT_ASSERT_EQUAL(shim->Captured.size(), 1);
        // send delivery problem, read should be restarted (it will be second retry attempt for this read)
        s.Runtime->Send(shim->Captured[0]->Sender, s.Sender, undelivery.Release());

        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->AllowResults();
        shim->SkipAll();

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, SortOrder::Ascending)), ",1,2,0,3,4,5,6");
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

    Y_UNIT_TEST(StreamLookupJoinRetryAttemptForFinishedRead) {
        TTestSetup s(ETestActorType::StreamLookup, "/Root/Table1");

        auto settings = MakeIntrusive<TIteratorReadBackoffSettings>();
        settings->StartRetryDelay = TDuration::MilliSeconds(250);
        settings->MaxShardAttempts = 4;
        // set small read response timeout (for frequent retries)
        settings->ReadResponseTimeout = TDuration::MilliSeconds(1);
        SetReadIteratorBackoffSettings(settings);

        ExecSQL(*s.Runtime, s.Sender, R"(
            --!syntax_v1
            CREATE TABLE `/Root/Table1` (Key uint64, Key2 uint64, Value uint64, PRIMARY KEY(Key, Key2));
        )", false);

        ExecSQL(*s.Runtime, s.Sender, R"(
            REPLACE INTO `/Root/Table1` (Key, Key2, Value) VALUES
                (1u, 1u, 10u);
        )", true);

        auto shards = s.Shards();
        auto* shim = new TReadActorPipeCacheStub();

        InterceptStreamLookupActorPipeCache(s.Runtime->Register(shim));
        shim->SetupCapture(0, 1);

        s.SendDataQuery(R"(
            $data = AsList(
                AsStruct(1u AS Key, 1u AS Value),
                AsStruct(2147483648u AS Key, 2147483648u AS Value));

            SELECT b.Value
            FROM AS_TABLE($data) a
            JOIN `/Root/Table1` b
            ON a.Key = b.Key
            ORDER BY b.Value ASC;
        )");

        shim->ReadsReceived.WaitI();

        UNIT_ASSERT_EQUAL(shards.size(), 1);
        auto undelivery = MakeHolder<TEvPipeCache::TEvDeliveryProblem>(shards[0], true);

        UNIT_ASSERT_EQUAL(shim->Captured.size(), 1);
        s.Runtime->Send(shim->Captured[0]->Sender, s.Sender, undelivery.Release());

        shim->SkipAll();

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, SortOrder::Ascending)), ",10");
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

    Y_UNIT_TEST(StreamLookupJoinDeliveryProblem) {
        TTestSetup s(ETestActorType::StreamLookup, "/Root/Table1");

        ExecSQL(*s.Runtime, s.Sender, R"(
            --!syntax_v1
            CREATE TABLE `/Root/Table1` (Key uint64, Key2 uint64, Value uint64, PRIMARY KEY(Key, Key2));
        )", false);

        ExecSQL(*s.Runtime, s.Sender, R"(
            REPLACE INTO `/Root/Table1` (Key, Key2, Value) VALUES
                (1u, 1u, 10u);
        )", true);

        auto shards = s.Shards();
        auto* shim = new TReadActorPipeCacheStub();

        InterceptStreamLookupActorPipeCache(s.Runtime->Register(shim));
        shim->SetupCapture(0, 1);

        s.SendDataQuery(R"(
            $data = AsList(
                AsStruct(1u AS Key, 1u AS Value),
                AsStruct(2147483648u AS Key, 2147483648u AS Value));

            SELECT b.Value
            FROM AS_TABLE($data) a
            JOIN `/Root/Table1` b
            ON a.Key = b.Key
            ORDER BY b.Value ASC;
        )");

        shim->ReadsReceived.WaitI();

        UNIT_ASSERT_EQUAL(shards.size(), 1);
        auto undelivery = MakeHolder<TEvPipeCache::TEvDeliveryProblem>(shards[0], true);

        UNIT_ASSERT_EQUAL(shim->Captured.size(), 1);
        s.Runtime->Send(shim->Captured[0]->Sender, s.Sender, undelivery.Release());

        shim->SkipAll();
        shim->SendCaptured(s.Runtime);

        s.AssertSuccess();
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(s.CollectedKeys, SortOrder::Ascending)), ",10");
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
