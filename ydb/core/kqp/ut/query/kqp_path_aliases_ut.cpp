#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/path_aliasing/path_normalizer.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;

struct TPathAliasesFixture {
    static TKikimrSettings MakeSettings(bool startupAliases) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        settings.SetEnableExternalDataSources(true);
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        if (startupAliases) {
            auto* rule = settings.AppConfig.MutableResourcePathPrefixMapping()->AddRules();
            rule->SetSrc("/Alias");
            rule->SetDst("/Root/Before");
        }
        return settings;
    }

    explicit TPathAliasesFixture(bool startupAliases = true)
        : Settings(MakeSettings(startupAliases))
        , Kikimr(Settings)
        , Session(CreateSession())
        , Edge(Runtime().AllocateEdgeActor())
        , Service(MakeKqpCompileServiceID(Runtime().GetNodeId()))
    {
        for (const auto* directory : {"/Root/Before", "/Root/After"}) {
            auto result = Kikimr.RunCall([&] {
                return Kikimr.GetSchemeClient().MakeDirectory(directory).GetValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        Scheme(R"(
            CREATE TABLE `/Root/Before/Rows` (Key Uint64 NOT NULL, PRIMARY KEY (Key));
            CREATE TABLE `/Root/After/Rows` (Key Uint64 NOT NULL, PRIMARY KEY (Key));
        )");
        auto result = Kikimr.RunCall([&] {
            return Session.ExecuteDataQuery(R"(
                UPSERT INTO `/Root/Before/Rows` (Key) VALUES (1u);
                UPSERT INTO `/Root/After/Rows` (Key) VALUES (2u);
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    TTestActorRuntime& Runtime() { return *Kikimr.GetTestServer().GetRuntime(); }

    NYdb::NTable::TSession CreateSession() {
        return Kikimr.RunCall([&] {
            return Kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        });
    }

    void Scheme(const TString& sql) {
        auto result = Kikimr.RunCall([&] { return Session.ExecuteSchemeQuery(sql).GetValueSync(); });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void CheckPath(const TString& path) {
        auto result = Kikimr.RunCall([&] { return Kikimr.GetSchemeClient().DescribePath(path).GetValueSync(); });
        UNIT_ASSERT_C(result.IsSuccess(), path << ": " << result.GetIssues().ToString());
    }

    THolder<NConsole::TEvConsole::TEvConfigNotificationRequest> Notification(TStringBuf destination) {
        auto notification = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        *notification->Record.MutableConfig()->MutableTableServiceConfig() = Settings.AppConfig.GetTableServiceConfig();
        std::shared_ptr<const NPathAliasing::TPathNormalizer> normalizer;
        if (!destination.empty()) {
            NKikimrConfig::TPathRewriteConfig mapping;
            auto* rule = mapping.AddRules();
            rule->SetSrc("/Alias");
            rule->SetDst(TString(destination));
            normalizer = std::make_shared<const NPathAliasing::TPathNormalizer>(mapping);
        }
        notification->PathNormalizer = std::move(normalizer);
        return notification;
    }

    void Update(TStringBuf destination) {
        Runtime().Send(new IEventHandle(Service, Edge, Notification(destination).Release()));
        UNIT_ASSERT(Runtime().GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(Edge, TDuration::Seconds(30)));
    }

    void CheckScript(const TString& path, const TString& expected, bool streaming = false) {
        NScripting::TScriptingClient client(Kikimr.GetDriver());
        const TString sql = TStringBuilder() << "SELECT Key FROM `" << path << "`;";
        if (streaming) {
            auto result = Kikimr.RunCall([&] {
                auto iterator = client.StreamExecuteYqlScript(sql).GetValueSync();
                UNIT_ASSERT_C(iterator.IsSuccess(), iterator.GetIssues().ToString());
                return CollectStreamResult(iterator);
            });
            CompareYson(expected, result.ResultSetYson);
        } else {
            auto result = Kikimr.RunCall([&] { return client.ExecuteYqlScript(sql).GetValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(expected, FormatResultSetYson(result.GetResultSets().front()));
        }
    }

    TKikimrSettings Settings;
    TKikimrRunner Kikimr;
    NYdb::NTable::TSession Session;
    TActorId Edge;
    TActorId Service;
};

} // namespace

Y_UNIT_TEST_SUITE(KqpWorkerPathAliases) {
    Y_UNIT_TEST_TWIN(UpdateExistingAndNewSessions, StartupAliases) {
        TPathAliasesFixture fixture(StartupAliases);
        const auto createObjects = [](TStringBuf prefix) -> TString {
            return TStringBuilder()
                << "CREATE TABLE `" << prefix << "/Created` (Key Uint64 NOT NULL, PRIMARY KEY (Key));"
                << "CREATE TOPIC `" << prefix << "/Topic`;"
                << "CREATE EXTERNAL DATA SOURCE `" << prefix << "/Source` WITH ("
                    "SOURCE_TYPE='ObjectStorage', LOCATION='my-bucket', AUTH_METHOD='NONE');"
                << "CREATE EXTERNAL TABLE `" << prefix << "/External` (Key Uint64) WITH ("
                    "DATA_SOURCE='" << prefix << "/Source', LOCATION='/');";
        };
        fixture.Scheme(createObjects(StartupAliases ? "/Alias" : "/Root/Before"));
        fixture.CheckScript(StartupAliases ? "/Alias/Rows" : "/Root/Before/Rows", "[[1u]]");

        fixture.Update("/Root/After");
        fixture.Scheme(createObjects("/Alias"));
        for (const auto* name : {"Created", "Topic", "Source", "External"}) {
            fixture.CheckPath(TStringBuilder() << "/Root/After/" << name);
        }
        auto description = Navigate(fixture.Runtime(), fixture.Edge, "/Root/After/External",
            NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        UNIT_ASSERT(description->ResultSet.front().ExternalTableInfo);
        UNIT_ASSERT_VALUES_EQUAL(description->ResultSet.front().ExternalTableInfo->Description.GetDataSourcePath(),
            "/Root/After/Source");
        fixture.CheckScript("/Alias/Rows", "[[2u]]");
        fixture.CheckScript("/Alias/Rows", "[[2u]]", true);

        auto session = fixture.CreateSession();
        auto result = fixture.Kikimr.RunCall([&] {
            return session.ExecuteSchemeQuery("CREATE TABLE `/Alias/NewWorker` (Key Uint64, PRIMARY KEY (Key));").GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        fixture.CheckPath("/Root/After/NewWorker");

        fixture.Update({});
        auto disabled = fixture.Kikimr.RunCall([&] {
            return fixture.Session.ExecuteSchemeQuery("CREATE TABLE `/Alias/Disabled` (Key Uint64, PRIMARY KEY (Key));").GetValueSync();
        });
        UNIT_ASSERT(!disabled.IsSuccess());
        fixture.CheckScript("/Root/After/Rows", "[[2u]]");
        NScripting::TScriptingClient client(fixture.Kikimr.GetDriver());
        auto script = fixture.Kikimr.RunCall([&] {
            return client.ExecuteYqlScript("SELECT Key FROM `/Alias/Rows`;").GetValueSync();
        });
        UNIT_ASSERT(!script.IsSuccess());

        fixture.Update("/Root/Before");
        fixture.Scheme("CREATE TABLE `/Alias/Reenabled` (Key Uint64, PRIMARY KEY (Key));");
        fixture.CheckPath("/Root/Before/Reenabled");
        fixture.CheckScript("/Alias/Rows", "[[1u]]");
    }

    Y_UNIT_TEST(DispatchedQueryKeepsItsAliases) {
        TPathAliasesFixture fixture;
        TAutoPtr<IEventHandle> held;
        bool switched = false;
        auto previous = fixture.Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!switched && ev->GetTypeRewrite() == TEvKqp::TEvWorkerQueryRequest::EventType
                && ev->Recipient != fixture.Service) {
                switched = true;
                held = ev.Release();
                fixture.Runtime().Send(new IEventHandle(fixture.Service, fixture.Edge,
                    fixture.Notification("/Root/After").Release()));
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (held && ev->GetTypeRewrite() == NConsole::TEvConsole::TEvConfigNotificationResponse::EventType
                && ev->Recipient == fixture.Edge) {
                fixture.Runtime().Send(held.Release());
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { fixture.Runtime().SetObserverFunc(previous); };

        fixture.CheckScript("/Alias/Rows", "[[1u]]");
        UNIT_ASSERT(switched);
        UNIT_ASSERT(!held);
        fixture.CheckScript("/Alias/Rows", "[[2u]]");
    }
}

} // namespace NKikimr::NKqp
