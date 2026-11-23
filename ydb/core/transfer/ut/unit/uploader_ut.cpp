#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/transfer/events.h>
#include <ydb/core/transfer/scheme.h>
#include <ydb/core/transfer/uploader.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NReplication::NTransfer;
using namespace NKikimr::NSchemeCache;

namespace {

using TData = TVector<std::pair<TSerializedCellVec, TString>>;

TScheme::TPtr MakeScheme() {
    TAutoPtr<TSchemeCacheNavigate> nav(new TSchemeCacheNavigate());
    auto& entry = nav->ResultSet.emplace_back();
    entry.Path = {"Root", "Table"};
    entry.Columns[1] = TSysTables::TTableColumnInfo(
        "Key", 1, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), {}, 0);
    entry.Columns[2] = TSysTables::TTableColumnInfo(
        "Message", 2, NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), {}, -1);
    entry.NotNullColumns.insert("Key");
    return BuildScheme(nav);
}

std::shared_ptr<TData> MakeRows() {
    auto data = std::make_shared<TData>();
    TVector<TCell> key{TCell::Make<ui64>(1)};
    data->emplace_back(TSerializedCellVec(key), TSerializedCellVec::Serialize({}));
    return data;
}

NYql::TIssues MakeIssues(const TString& message) {
    NYql::TIssues issues;
    issues.AddIssue(message);
    return issues;
}

struct TUploaderEnv {
    TTestBasicRuntime Runtime;
    TActorId Edge;
    TActorId Uploader;
    std::vector<TActorId> Children;
    TScheme::TPtr Scheme = MakeScheme();

    void Pump() {
        const auto prev = Runtime.SetDispatchTimeout(TDuration::MilliSeconds(50));
        try {
            Runtime.DispatchEvents();
        } catch (const TEmptyEventQueueException&) {
        } catch (const TSchedulingLimitReachedException&) {
        }
        Runtime.SetDispatchTimeout(prev);
    }

    TUploaderEnv()
        : Runtime(1, false)
    {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Runtime.SetLogPriority(NKikimrServices::TRANSFER, NLog::PRI_DEBUG);
        Runtime.SetRegistrationObserverFunc([this](TTestActorRuntimeBase& rt, const TActorId& parent, const TActorId& actorId) {
            rt.EnableScheduleForActor(actorId);
            if (Uploader && parent == Uploader) {
                Children.push_back(actorId);
            }
        });
        Edge = Runtime.AllocateEdgeActor();
    }

    void Start(std::unordered_map<TString, std::shared_ptr<TData>> data) {
        Uploader = Runtime.Register(new TTableUploader<TData>(
            Edge, "/Root", "/Root/Table", Scheme, std::move(data)));
        Runtime.EnableScheduleForActor(Uploader);
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTxUserProxy::TEvUploadRowsResponse::EventType
                && ev->Sender != Edge)
            {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Pump();
        DropChildren();
    }

    void DropChildren() {
        for (const auto& child : Children) {
            Runtime.Send(new IEventHandle(child, Edge, new TEvents::TEvPoison()));
        }
        Children.clear();
        Pump();
    }

    void SendResponse(ui64 cookie, Ydb::StatusIds::StatusCode status, const TString& issue = {}) {
        Runtime.Send(new IEventHandle(
            Uploader,
            Edge,
            new TEvTxUserProxy::TEvUploadRowsResponse(status, MakeIssues(issue)),
            0,
            cookie));
        Pump();
    }

    THolder<NTransferPrivate::TEvWriteCompleeted> WaitCompleted() {
        Pump();
        auto ev = Runtime.GrabEdgeEvent<NTransferPrivate::TEvWriteCompleeted>(TDuration::Zero());
        UNIT_ASSERT(ev);
        return ev;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TransferUploader) {

Y_UNIT_TEST(UnknownCookieIsIgnoredThenSuccessCompletes) {
    TUploaderEnv env;
    env.Start({{"/Root/Table", MakeRows()}});
    env.SendResponse(999, Ydb::StatusIds::SUCCESS);
    env.SendResponse(1, Ydb::StatusIds::SUCCESS);
    auto done = env.WaitCompleted();
    UNIT_ASSERT_VALUES_EQUAL(done->Status, Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(TwoTablesCompleteAfterBothSuccesses) {
    TUploaderEnv env;
    env.Start({
        {"/Root/Table", MakeRows()},
        {"/Root/Other", MakeRows()},
    });
    env.SendResponse(1, Ydb::StatusIds::SUCCESS);
    env.SendResponse(2, Ydb::StatusIds::SUCCESS);
    auto done = env.WaitCompleted();
    UNIT_ASSERT_VALUES_EQUAL(done->Status, Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(UnknownTableRetriesWithDefaultThenFails) {
    TUploaderEnv env;
    env.Start({{"/Root/Missing", MakeRows()}});
    env.SendResponse(1, Ydb::StatusIds::SCHEME_ERROR, "unknown table: /Root/Missing");
    env.Runtime.AdvanceCurrentTime(TDuration::Seconds(2));
    env.Pump();
    env.DropChildren();
    env.SendResponse(2, Ydb::StatusIds::SCHEME_ERROR, "unknown table: /Root/Table");
    auto done = env.WaitCompleted();
    UNIT_ASSERT_VALUES_EQUAL(done->Status, Ydb::StatusIds::SCHEME_ERROR);
}

Y_UNIT_TEST(OltpMismatchFailsImmediately) {
    TUploaderEnv env;
    env.Start({{"/Root/Table", MakeRows()}});
    env.SendResponse(1, Ydb::StatusIds::SCHEME_ERROR, "Only the OLTP table is supported");
    auto done = env.WaitCompleted();
    UNIT_ASSERT_VALUES_EQUAL(done->Status, Ydb::StatusIds::SCHEME_ERROR);
}

Y_UNIT_TEST(OlapMismatchFailsImmediately) {
    TUploaderEnv env;
    env.Start({{"/Root/Table", MakeRows()}});
    env.SendResponse(1, Ydb::StatusIds::SCHEME_ERROR, "Only the OLAP table is supported");
    auto done = env.WaitCompleted();
    UNIT_ASSERT_VALUES_EQUAL(done->Status, Ydb::StatusIds::SCHEME_ERROR);
}

Y_UNIT_TEST(SchemeErrorsExhaustRetries) {
    TUploaderEnv env;
    env.Start({{"/Root/Table", MakeRows()}});
    for (ui64 cookie = 1; cookie <= 4; ++cookie) {
        env.SendResponse(cookie, Ydb::StatusIds::BAD_REQUEST, "bad request");
        if (cookie < 4) {
            env.Runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            env.Pump();
            env.DropChildren();
        }
    }
    auto done = env.WaitCompleted();
    UNIT_ASSERT_VALUES_EQUAL(done->Status, Ydb::StatusIds::BAD_REQUEST);
}

Y_UNIT_TEST(TransientErrorRetriesThenSucceeds) {
    TUploaderEnv env;
    env.Start({{"/Root/Table", MakeRows()}});
    env.SendResponse(1, Ydb::StatusIds::UNAVAILABLE, "unavailable");
    env.Runtime.AdvanceCurrentTime(TDuration::Seconds(2));
    env.Pump();
    env.DropChildren();
    env.SendResponse(2, Ydb::StatusIds::SUCCESS);
    auto done = env.WaitCompleted();
    UNIT_ASSERT_VALUES_EQUAL(done->Status, Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(UnexpectedRetryFails) {
    TUploaderEnv env;
    env.Start({{"/Root/Table", MakeRows()}});
    env.Runtime.Send(new IEventHandle(
        env.Uploader,
        env.Edge,
        new NTransferPrivate::TEvRetryTable("/Root/Unknown", false)));
    env.Pump();
    auto done = env.WaitCompleted();
    UNIT_ASSERT_VALUES_EQUAL(done->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(done->Issues.ToOneLineString(), "Unexpected retry");
}

Y_UNIT_TEST(PoisonCancelsInFlightUploads) {
    TUploaderEnv env;
    env.Start({{"/Root/Table", MakeRows()}});
    env.Runtime.Send(new IEventHandle(env.Uploader, env.Edge, new TEvents::TEvPoison()));
    env.Pump();
}

} // Y_UNIT_TEST_SUITE(TransferUploader)
