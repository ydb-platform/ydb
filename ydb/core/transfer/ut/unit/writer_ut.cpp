#include <ydb/core/base/appdata.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/transfer/events.h>
#include <ydb/core/transfer/purecalc.h>
#include <ydb/core/transfer/scheme.h>
#include <ydb/core/transfer/table_kind_state.h>
#include <ydb/core/transfer/transfer_writer.h>
#include <ydb/core/tx/replication/service/worker.h>
#include <ydb/core/tx/replication/ydb_proxy/topic_message.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/services/services.pb.h>

#include <yql/essentials/public/purecalc/common/interface.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NReplication;
using namespace NKikimr::NReplication::NService;
using namespace NKikimr::NReplication::NTransfer;
using namespace NKikimr::NSchemeCache;
using namespace NFq;

namespace {

using TNavigate = TSchemeCacheNavigate;

TString DefaultLambda() {
    return R"(
$__ydb_transfer_lambda = ($x) -> {
    return [
        <|
            Key: $x._offset,
            Message: CAST($x._data AS Utf8)
        |>
    ];
};
)";
}

TString TableLambda(const TString& table = "other") {
    return TStringBuilder() << R"(
$__ydb_transfer_lambda = ($x) -> {
    return [
        <|
            __ydb_table: ")" << table << R"(",
            Key: $x._offset,
            Message: CAST($x._data AS Utf8)
        |>
    ];
};
)";
}

TString EmptyBatchLambda() {
    return R"(
$__ydb_transfer_lambda = ($x) -> {
    $row = <|
        Key: $x._offset,
        Message: CAST($x._data AS Utf8)
    |>;
    RETURN ListFilter([$row], ($r) -> { RETURN false; });
};
)";
}

TString UnwrapErrorLambda() {
    return R"(
$__ydb_transfer_lambda = ($x) -> {
    return [
        <|
            Key: $x._offset,
            Message: Unwrap(Nothing(Utf8?), "unwrap error")
        |>
    ];
};
)";
}

void FillOkTable(TNavigate::TEntry& entry, bool columnTable = false) {
    entry.Status = TNavigate::EStatus::Ok;
    entry.Kind = columnTable ? TNavigate::KindColumnTable : TNavigate::KindTable;
    if (entry.Path.empty()) {
        entry.Path = {"Root", "Table"};
    }
    entry.Columns[1] = TSysTables::TTableColumnInfo(
        "Key", 1, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), {}, 0);
    entry.Columns[2] = TSysTables::TTableColumnInfo(
        "Message", 2, NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), {}, -1);
    entry.NotNullColumns.insert("Key");
}

struct TSchemeRequest {
    TAutoPtr<TNavigate> Request;
    TActorId Sender;
    bool Pending = false;
};

class TFakeSchemeCacheActor: public TActorBootstrapped<TFakeSchemeCacheActor> {
public:
    using TFiller = std::function<void(TNavigate&)>;

    TFakeSchemeCacheActor(TFiller filler, std::shared_ptr<TSchemeRequest> delayed)
        : Filler(std::move(filler))
        , Delayed(std::move(delayed))
    {
    }

    void Bootstrap() {
        Become(&TFakeSchemeCacheActor::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
        IgnoreFunc(TEvTxProxySchemeCache::TEvResolveKeySet);
    )

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
        auto request = std::move(ev->Get()->Request);
        if (Delayed) {
            Delayed->Request = std::move(request);
            Delayed->Sender = ev->Sender;
            Delayed->Pending = true;
            return;
        }
        if (request && Filler) {
            Filler(*request);
        }
        Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request)));
    }

    TFiller Filler;
    std::shared_ptr<TSchemeRequest> Delayed;
};

enum class ECompileMode {
    Succeed,
    Fail,
    Hold,
};

struct TCompileRequest {
    NFq::IProgramHolder::TPtr Holder;
    TActorId Sender;
    ui64 Cookie = 0;
    bool Pending = false;
};

class TFakeCompileActor: public TActorBootstrapped<TFakeCompileActor> {
public:
    NYql::NPureCalc::IProgramFactoryPtr MakeFactory() {
        auto options = NYql::NPureCalc::TProgramFactoryOptions();
        options.SetLLVMSettings("OFF");
        return NYql::NPureCalc::MakeProgramFactory(options);
    }

    TFakeCompileActor(ECompileMode mode, std::shared_ptr<TCompileRequest> held)
        : Mode(mode)
        , Held(std::move(held))
        , ProgramFactory(MakeFactory())
    {
    }

    void Bootstrap() {
        Become(&TFakeCompileActor::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvRowDispatcher::TEvPurecalcCompileRequest, Handle);
        IgnoreFunc(TEvRowDispatcher::TEvPurecalcCompileAbort);
    )

private:
    void Handle(TEvRowDispatcher::TEvPurecalcCompileRequest::TPtr& ev) {
        if (Mode == ECompileMode::Fail) {
            NYql::TIssues issues;
            issues.AddIssue("compile failed");
            Send(ev->Sender, new TEvRowDispatcher::TEvPurecalcCompileResponse(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR, std::move(issues)), 0, ev->Cookie);
            return;
        }

        if (Mode == ECompileMode::Hold) {
            Held->Holder = std::move(ev->Get()->ProgramHolder);
            Held->Sender = ev->Sender;
            Held->Cookie = ev->Cookie;
            Held->Pending = true;
            return;
        }

        RespondSuccess(ev->Sender, ev->Cookie, std::move(ev->Get()->ProgramHolder));
    }

    void RespondSuccess(const TActorId& sender, ui64 cookie, NFq::IProgramHolder::TPtr holder) {
        try {
            holder->CreateProgram(ProgramFactory);
        } catch (const NYql::NPureCalc::TCompileError& e) {
            NYql::TIssues issues;
            issues.AddIssue(e.GetIssues());
            Send(sender, new TEvRowDispatcher::TEvPurecalcCompileResponse(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR, std::move(issues)), 0, cookie);
            return;
        }
        Send(sender, new TEvRowDispatcher::TEvPurecalcCompileResponse(std::move(holder)), 0, cookie);
    }

    const ECompileMode Mode;
    std::shared_ptr<TCompileRequest> Held;
    NYql::NPureCalc::IProgramFactoryPtr ProgramFactory;
};

NKikimrReplication::TBatchingSettings MakeBatching(ui64 flushMs = 1000, ui64 batchBytes = 1_MB) {
    NKikimrReplication::TBatchingSettings settings;
    settings.SetFlushIntervalMilliSeconds(flushMs);
    settings.SetBatchSizeBytes(batchBytes);
    return settings;
}

IActor* CreateWriter(
    const TString& lambda,
    const TPathId& pathId,
    const TActorId& compileService,
    const TString& directory = {},
    const TString& database = "/Root",
    const NKikimrReplication::TBatchingSettings& batching = MakeBatching())
{
    TString runAsUser;
    TTransferWriterFactory factory;
    ITransferWriterFactory::Parameters params{
        lambda, pathId, compileService, batching, runAsUser, directory, database};
    return factory.Create(params);
}

struct TWriterEnv {
    TTestBasicRuntime Runtime;
    TActorId Edge;
    TActorId Compile;
    TPathId TablePathId = TPathId(1, 2);
    std::shared_ptr<TSchemeRequest> DelayedScheme;
    std::shared_ptr<TCompileRequest> HeldCompile;

    void Pump(TDuration dispatchTimeout = TDuration::Zero()) {
        const auto prev = Runtime.SetDispatchTimeout(dispatchTimeout);
        try {
            Runtime.DispatchEvents();
        } catch (const TEmptyEventQueueException&) {
        } catch (const TSchedulingLimitReachedException&) {
        }
        Runtime.SetDispatchTimeout(prev);
    }

    TWriterEnv(
        TFakeSchemeCacheActor::TFiller filler = {},
        ECompileMode compileMode = ECompileMode::Succeed,
        bool delayScheme = false)
        : Runtime(1, false)
    {
        if (!filler) {
            filler = [](TNavigate& nav) {
                if (nav.ResultSet.empty()) {
                    nav.ResultSet.emplace_back();
                }
                FillOkTable(nav.ResultSet[0]);
            };
        }
        if (delayScheme) {
            DelayedScheme = std::make_shared<TSchemeRequest>();
        }
        if (compileMode == ECompileMode::Hold) {
            HeldCompile = std::make_shared<TCompileRequest>();
        }

        Runtime.Initialize(TAppPrepare().Unwrap());
        Runtime.SetLogPriority(NKikimrServices::TRANSFER, NLog::PRI_DEBUG);
        Runtime.SetDispatchTimeout(TDuration::MilliSeconds(50));
        Runtime.SetObserverFunc([](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTxUserProxy::TEvUploadRowsResponse::EventType
                || ev->GetTypeRewrite() == TEvTxProxySchemeCache::TEvResolveKeySet::EventType)
            {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto schemeCacheId = Runtime.Register(new TFakeSchemeCacheActor(std::move(filler), DelayedScheme));
        Runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId);

        Compile = Runtime.Register(new TFakeCompileActor(compileMode, HeldCompile));
        Edge = Runtime.AllocateEdgeActor();
        Pump();
    }

    TActorId StartWriter(
        const TString& lambda = DefaultLambda(),
        const TString& directory = {},
        const TString& database = "/Root",
        const NKikimrReplication::TBatchingSettings& batching = MakeBatching(),
        bool enableSchedule = false)
    {
        auto writer = Runtime.Register(CreateWriter(lambda, TablePathId, Compile, directory, database, batching));
        if (enableSchedule) {
            Runtime.EnableScheduleForActor(writer);
        }
        Pump();
        return writer;
    }

    void ReplyDelayedScheme(TFakeSchemeCacheActor::TFiller filler = {}) {
        UNIT_ASSERT(DelayedScheme && DelayedScheme->Pending);
        if (!filler) {
            filler = [](TNavigate& nav) {
                if (nav.ResultSet.empty()) {
                    nav.ResultSet.emplace_back();
                }
                FillOkTable(nav.ResultSet[0]);
            };
        }
        filler(*DelayedScheme->Request);
        Runtime.Send(new IEventHandle(
            DelayedScheme->Sender,
            Edge,
            new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(DelayedScheme->Request))));
        DelayedScheme->Pending = false;
        Pump();
    }

    void ReplyHeldCompile(bool success = true) {
        UNIT_ASSERT(HeldCompile && HeldCompile->Pending);
        if (success) {
            auto options = NYql::NPureCalc::TProgramFactoryOptions();
            options.SetLLVMSettings("OFF");
            auto factory = NYql::NPureCalc::MakeProgramFactory(options);
            HeldCompile->Holder->CreateProgram(factory);
            Runtime.Send(new IEventHandle(
                HeldCompile->Sender,
                Edge,
                new TEvRowDispatcher::TEvPurecalcCompileResponse(std::move(HeldCompile->Holder)),
                0,
                HeldCompile->Cookie));
        } else {
            NYql::TIssues issues;
            issues.AddIssue("held compile failed");
            Runtime.Send(new IEventHandle(
                HeldCompile->Sender,
                Edge,
                new TEvRowDispatcher::TEvPurecalcCompileResponse(
                    NYql::NDqProto::StatusIds::INTERNAL_ERROR, std::move(issues)),
                0,
                HeldCompile->Cookie));
        }
        HeldCompile->Pending = false;
        Pump();
    }

    void Kick(const TActorId& writer) {
        Runtime.Send(new IEventHandle(writer, Edge, new TEvWorker::TEvHandshake()));
    }

    void Handshake(const TActorId& writer) {
        Kick(writer);
        Pump();
        auto ev = Runtime.GrabEdgeEvent<TEvWorker::TEvHandshake>(TDuration::Zero());
        UNIT_ASSERT(ev);
    }

    THolder<TEvWorker::TEvGone> WaitGone() {
        Pump();
        auto ev = Runtime.GrabEdgeEvent<TEvWorker::TEvGone>(TDuration::Zero());
        UNIT_ASSERT(ev);
        return ev;
    }

    template <typename TEvent>
    THolder<TEvent> Grab() {
        Pump();
        auto ev = Runtime.GrabEdgeEvent<TEvent>(TDuration::Zero());
        UNIT_ASSERT(ev);
        return ev;
    }

    void SendData(const TActorId& writer, TVector<TTopicMessage> records, ui32 partitionId = 0) {
        Runtime.Send(new IEventHandle(writer, Edge, new TEvWorker::TEvData(partitionId, "src", std::move(records))));
        Pump();
    }
};

class TFlushProbeActor: public TActorBootstrapped<TFlushProbeActor> {
public:
    TFlushProbeActor(const TActorId& edge, TAutoPtr<TNavigate> nav, bool column)
        : Edge(edge)
        , Nav(std::move(nav))
        , Column(column)
    {
    }

    void Bootstrap() {
        auto state = Column
            ? CreateColumnTableState(SelfId(), "/Root", "/Root/Table", Nav)
            : CreateRowTableState(SelfId(), "/Root", "/Root/Table", Nav);
        UNIT_ASSERT(!state->Flush());
        UNIT_ASSERT_VALUES_EQUAL(state->BatchSize(), 0u);
        state->PassAway();
        Send(Edge, new TEvents::TEvWakeup());
        PassAway();
    }

private:
    const TActorId Edge;
    TAutoPtr<TNavigate> Nav;
    const bool Column;
};

} // namespace

Y_UNIT_TEST_SUITE(TransferWriter) {

Y_UNIT_TEST(EmptyNavigateResultLeaves) {
    TWriterEnv env([](TNavigate& nav) {
        nav.ResultSet.clear();
    });
    auto writer = env.StartWriter();
    env.Kick(writer);
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
}

Y_UNIT_TEST(EmptyNavigatePointerLeaves) {
    TWriterEnv env({}, ECompileMode::Fail, /*delayScheme=*/true);
    auto writer = env.StartWriter();
    UNIT_ASSERT(env.DelayedScheme && env.DelayedScheme->Pending);
    env.Runtime.Send(new IEventHandle(
        env.DelayedScheme->Sender,
        env.Edge,
        new TEvTxProxySchemeCache::TEvNavigateKeySetResult(TAutoPtr<TNavigate>())));
    env.DelayedScheme->Pending = false;
    env.Pump();
    env.Runtime.Send(new IEventHandle(writer, env.Edge, new TEvWorker::TEvHandshake()));
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
}

Y_UNIT_TEST(UnexpectedEntriesCountLeaves) {
    TWriterEnv env([](TNavigate& nav) {
        if (nav.ResultSet.empty()) {
            nav.ResultSet.emplace_back();
        }
        FillOkTable(nav.ResultSet[0]);
        nav.ResultSet.emplace_back();
        FillOkTable(nav.ResultSet[1]);
    });
    auto writer = env.StartWriter();
    env.Kick(writer);
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
}

Y_UNIT_TEST(TableIdMismatchLeaves) {
    TWriterEnv env([](TNavigate& nav) {
        if (nav.ResultSet.empty()) {
            nav.ResultSet.emplace_back();
        }
        FillOkTable(nav.ResultSet[0]);
        nav.ResultSet[0].TableId = TTableId(TPathId(9, 9));
    });
    auto writer = env.StartWriter();
    env.Kick(writer);
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
}

Y_UNIT_TEST(PathNotTableLeaves) {
    TWriterEnv env([](TNavigate& nav) {
        if (nav.ResultSet.empty()) {
            nav.ResultSet.emplace_back();
        }
        nav.ResultSet[0].Status = TNavigate::EStatus::PathNotTable;
        nav.ResultSet[0].Kind = TNavigate::KindTable;
    });
    auto writer = env.StartWriter();
    env.Kick(writer);
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(gone->ErrorDescription, "Only tables are supported");
}

Y_UNIT_TEST(NonTableKindLeaves) {
    TWriterEnv env([](TNavigate& nav) {
        if (nav.ResultSet.empty()) {
            nav.ResultSet.emplace_back();
        }
        nav.ResultSet[0].Status = TNavigate::EStatus::Ok;
        nav.ResultSet[0].Kind = TNavigate::KindTopic;
    });
    auto writer = env.StartWriter();
    env.Kick(writer);
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(gone->ErrorDescription, "Only tables are supported");
}

Y_UNIT_TEST(LookupErrorRetriesThenSucceeds) {
    ui32 requests = 0;
    TWriterEnv env([&](TNavigate& nav) {
        if (nav.ResultSet.empty()) {
            nav.ResultSet.emplace_back();
        }
        if (requests++ == 0) {
            nav.ResultSet[0].Status = TNavigate::EStatus::LookupError;
            nav.ResultSet[0].Kind = TNavigate::KindTable;
            return;
        }
        FillOkTable(nav.ResultSet[0]);
    });
    auto writer = env.StartWriter();
    env.Runtime.Send(new IEventHandle(writer, env.Edge, new TEvents::TEvWakeup()));
    env.Pump();
    env.Handshake(writer);
    env.SendData(writer, {});
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::DONE);
    UNIT_ASSERT_VALUES_EQUAL(requests, 2u);
}

Y_UNIT_TEST(CompilationFailedBeforeHandshake) {
    TWriterEnv env({}, ECompileMode::Fail);
    auto writer = env.StartWriter();
    env.Runtime.Send(new IEventHandle(writer, env.Edge, new TEvWorker::TEvHandshake()));
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(gone->ErrorDescription, "Compilation failed");
}

Y_UNIT_TEST(OutdatedCompileResponseIsIgnored) {
    TWriterEnv env({}, ECompileMode::Hold);
    auto writer = env.StartWriter();
    UNIT_ASSERT(env.HeldCompile && env.HeldCompile->Pending);

    NYql::TIssues issues;
    issues.AddIssue("outdated");
    env.Runtime.Send(new IEventHandle(
        env.HeldCompile->Sender,
        env.Edge,
        new TEvRowDispatcher::TEvPurecalcCompileResponse(
            NYql::NDqProto::StatusIds::INTERNAL_ERROR, std::move(issues)),
        0,
        env.HeldCompile->Cookie - 1));
    env.Pump();

    env.ReplyHeldCompile(true);
    env.Handshake(writer);
    env.SendData(writer, {});
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::DONE);
}

Y_UNIT_TEST(DataHeldDuringCompileIsProcessed) {
    TWriterEnv env({}, ECompileMode::Hold);
    auto writer = env.StartWriter();
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    env.ReplyHeldCompile(true);
    env.Grab<TEvWorker::TEvPoll>();
}

Y_UNIT_TEST(EmptyRecordsCompletePartition) {
    TWriterEnv env;
    auto writer = env.StartWriter();
    env.Handshake(writer);
    env.SendData(writer, {});
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::DONE);
}

Y_UNIT_TEST(EmptyTransformBatchCommitsOffset) {
    TWriterEnv env;
    auto writer = env.StartWriter(EmptyBatchLambda());
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(7, "skip")});
    auto commit = env.Grab<TEvWorker::TEvCommit>();
    UNIT_ASSERT_VALUES_EQUAL(commit->Offset, 8u);
}

Y_UNIT_TEST(TransformExceptionLeaves) {
    TWriterEnv env;
    auto writer = env.StartWriter(UnwrapErrorLambda());
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(gone->ErrorDescription, "unwrap error");
}

Y_UNIT_TEST(TableOverrideWithoutDirectoryLeaves) {
    TWriterEnv env;
    auto writer = env.StartWriter(TableLambda());
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(gone->ErrorDescription, "it is not allowed to specify a table to write");
}

Y_UNIT_TEST(TableOverrideOutsideDirectoryLeaves) {
    TWriterEnv env;
    auto writer = env.StartWriter(TableLambda("../outside"), "/Root/dir");
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(gone->ErrorDescription, "outside target directory");
}

Y_UNIT_TEST(DirectoryUnderDatabaseIsAccepted) {
    TWriterEnv env;
    auto writer = env.StartWriter(DefaultLambda(), "/Root/dir", "/Root");
    env.Handshake(writer);
    env.SendData(writer, {});
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::DONE);
}

Y_UNIT_TEST(RelativeDirectoryPathThrowsInConstructor) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        CreateWriter(DefaultLambda(), TPathId(1, 2), TActorId(), "dir", "/Root"),
        yexception,
        "No common parts");
}

Y_UNIT_TEST(DirectoryOutsideDatabaseIsIgnored) {
    TWriterEnv env;
    auto writer = env.StartWriter(TableLambda(), "/Other/dir", "/Root");
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(gone->ErrorDescription, "it is not allowed to specify a table to write");
}

Y_UNIT_TEST(FlushTimeoutSchedulesWrite) {
    TWriterEnv env;
    auto writer = env.StartWriter(DefaultLambda(), {}, "/Root", MakeBatching(1000, 8_GB));
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    env.Grab<TEvWorker::TEvPoll>();
    env.Runtime.EnableScheduleForActor(writer);
    env.Runtime.AdvanceCurrentTime(TDuration::Seconds(2));
    env.Pump(TDuration::MilliSeconds(50));
}

Y_UNIT_TEST(PoisonDuringSchemeLookup) {
    TWriterEnv env({}, ECompileMode::Succeed, /*delayScheme=*/true);
    auto writer = env.StartWriter();
    env.Runtime.Send(new IEventHandle(writer, env.Edge, new TEvents::TEvPoison()));
    env.Pump();
}

Y_UNIT_TEST(ColumnTableFlushEmpty) {
    TWriterEnv env([](TNavigate& nav) {
        if (nav.ResultSet.empty()) {
            nav.ResultSet.emplace_back();
        }
        FillOkTable(nav.ResultSet[0], true);
    });
    auto writer = env.StartWriter();
    env.Handshake(writer);
    env.SendData(writer, {});
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::DONE);
}

Y_UNIT_TEST(WriteCompletedErrorLeaves) {
    TWriterEnv env;
    auto writer = env.StartWriter(DefaultLambda(), {}, "/Root", MakeBatching(1000, 0));
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    NYql::TIssues issues;
    issues.AddIssue("upload failed");
    env.Runtime.Send(new IEventHandle(
        writer,
        env.Edge,
        new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::UNAVAILABLE, std::move(issues))));
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(gone->ErrorDescription, "upload failed");
}

Y_UNIT_TEST(WriteCompletedSuccessCommits) {
    TWriterEnv env;
    auto writer = env.StartWriter(DefaultLambda(), {}, "/Root", MakeBatching(1000, 0));
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(3, "hello")});
    NYql::TIssues issues;
    env.Runtime.Send(new IEventHandle(
        writer,
        env.Edge,
        new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::SUCCESS, std::move(issues))));
    auto commit = env.Grab<TEvWorker::TEvCommit>();
    UNIT_ASSERT_VALUES_EQUAL(commit->Offset, 4u);
}

Y_UNIT_TEST(LeaveOnBatchSizeExceedWakeup) {
    TWriterEnv env;
    auto writer = env.StartWriter(DefaultLambda(), {}, "/Root", MakeBatching(1000, 0));
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    env.Runtime.Send(new IEventHandle(writer, env.Edge, new TEvents::TEvWakeup(2)));
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::OVERLOAD);
    UNIT_ASSERT_STRING_CONTAINS(gone->ErrorDescription, "max batch size exceeded");
}

Y_UNIT_TEST(RetryFlushWakeupDuringWrite) {
    TWriterEnv env;
    auto writer = env.StartWriter(DefaultLambda(), {}, "/Root", MakeBatching(1000, 0));
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    env.Runtime.Send(new IEventHandle(writer, env.Edge, new TEvents::TEvWakeup(1)));
    env.Pump();
    NYql::TIssues issues;
    env.Runtime.Send(new IEventHandle(
        writer,
        env.Edge,
        new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::SUCCESS, std::move(issues))));
    env.Grab<TEvWorker::TEvCommit>();
}

Y_UNIT_TEST(FlushTimeoutWakeupDuringWrite) {
    TWriterEnv env;
    auto writer = env.StartWriter(DefaultLambda(), {}, "/Root", MakeBatching(1000, 0));
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    env.Runtime.Send(new IEventHandle(writer, env.Edge, new TEvents::TEvWakeup(0)));
    env.Pump();
    NYql::TIssues issues;
    env.Runtime.Send(new IEventHandle(
        writer,
        env.Edge,
        new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::SUCCESS, std::move(issues))));
    env.Grab<TEvWorker::TEvCommit>();
}

Y_UNIT_TEST(DataHeldDuringWriteIsProcessedAfterSuccess) {
    TWriterEnv env;
    auto writer = env.StartWriter(DefaultLambda(), {}, "/Root", MakeBatching(1000, 0));
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "first")});
    env.SendData(writer, {TTopicMessage(1, "second")});
    NYql::TIssues issues;
    env.Runtime.Send(new IEventHandle(
        writer,
        env.Edge,
        new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::SUCCESS, std::move(issues))));
    env.Grab<TEvWorker::TEvCommit>();
}

Y_UNIT_TEST(RowAndColumnFlushWithoutData) {
    TAutoPtr<TNavigate> rowNav(new TNavigate());
    rowNav->ResultSet.emplace_back();
    FillOkTable(rowNav->ResultSet[0], false);
    TAutoPtr<TNavigate> columnNav(new TNavigate());
    columnNav->ResultSet.emplace_back();
    FillOkTable(columnNav->ResultSet[0], true);

    TTestBasicRuntime runtime(1, false);
    runtime.Initialize(TAppPrepare().Unwrap());
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(new TFlushProbeActor(edge, std::move(rowNav), false));
    runtime.GrabEdgeEvent<TEvents::TEvWakeup>(TDuration::MilliSeconds(50));
    runtime.Register(new TFlushProbeActor(edge, std::move(columnNav), true));
    runtime.GrabEdgeEvent<TEvents::TEvWakeup>(TDuration::MilliSeconds(50));
}

Y_UNIT_TEST(DataHeldDuringSchemeLookup) {
    TWriterEnv env({}, ECompileMode::Succeed, /*delayScheme=*/true);
    auto writer = env.StartWriter();
    env.Kick(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    env.ReplyDelayedScheme();
    env.Grab<TEvWorker::TEvHandshake>();
    env.Grab<TEvWorker::TEvPoll>();
}

Y_UNIT_TEST(PoisonDuringCompile) {
    TWriterEnv env({}, ECompileMode::Hold);
    auto writer = env.StartWriter();
    env.Runtime.Send(new IEventHandle(writer, env.Edge, new TEvents::TEvPoison()));
    env.Pump();
}

Y_UNIT_TEST(WriteCompletedEmptyIssuesDoesNotLeave) {
    TWriterEnv env;
    auto writer = env.StartWriter(DefaultLambda(), {}, "/Root", MakeBatching(1000, 0));
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    NYql::TIssues issues;
    env.Runtime.Send(new IEventHandle(
        writer,
        env.Edge,
        new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::UNAVAILABLE, std::move(issues))));
    env.Grab<TEvWorker::TEvCommit>();
}

Y_UNIT_TEST(TableOverrideInsideDirectoryFlushes) {
    TWriterEnv env;
    auto writer = env.StartWriter(TableLambda("inner"), "/Root/dir", "/Root", MakeBatching(1000, 0));
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    NYql::TIssues issues;
    env.Runtime.Send(new IEventHandle(
        writer,
        env.Edge,
        new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::SUCCESS, std::move(issues))));
    env.Grab<TEvWorker::TEvCommit>();
}

Y_UNIT_TEST(ColumnTableWriteCompleted) {
    TWriterEnv env([](TNavigate& nav) {
        if (nav.ResultSet.empty()) {
            nav.ResultSet.emplace_back();
        }
        FillOkTable(nav.ResultSet[0], true);
    });
    auto writer = env.StartWriter(DefaultLambda(), {}, "/Root", MakeBatching(1000, 0));
    env.Handshake(writer);
    env.SendData(writer, {TTopicMessage(0, "hello")});
    NYql::TIssues issues;
    env.Runtime.Send(new IEventHandle(
        writer,
        env.Edge,
        new NTransferPrivate::TEvWriteCompleeted(Ydb::StatusIds::SUCCESS, std::move(issues))));
    env.Grab<TEvWorker::TEvCommit>();
}

Y_UNIT_TEST(CompileAbortIsIgnored) {
    TWriterEnv env({}, ECompileMode::Hold);
    auto writer = env.StartWriter();
    env.Runtime.Send(new IEventHandle(
        env.Compile,
        env.Edge,
        new TEvRowDispatcher::TEvPurecalcCompileAbort()));
    env.Pump();
    env.ReplyHeldCompile(true);
    env.Handshake(writer);
    env.SendData(writer, {});
    auto gone = env.WaitGone();
    UNIT_ASSERT_VALUES_EQUAL(gone->Status, TEvWorker::TEvGone::DONE);
}

} // Y_UNIT_TEST_SUITE(TransferWriter)
