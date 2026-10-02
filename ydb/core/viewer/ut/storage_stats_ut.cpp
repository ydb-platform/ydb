#include <ydb/core/testlib/test_client.h>
#include <ydb/core/viewer/ut/ut_utils.h>
#include <ydb/core/viewer/viewer_storage_stats.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

namespace NKikimr::NViewer {
namespace {

class TMonPage : public NMonitoring::IMonPage {
public:
    TMonPage()
        : IMonPage("viewer", "viewer")
    {}

    void Output(NMonitoring::IMonHttpRequest&) override {}
};

// Supply a successful navigate response with independently controlled DomainInfo.
class TStorageStatsWithNavigate : public TJsonStorageStats {
    const std::optional<TPathId> DomainKey;

public:
    TStorageStatsWithNavigate(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev, std::optional<TPathId> domainKey)
        : TJsonStorageStats(viewer, ev)
        , DomainKey(domainKey)
    {}

    void Bootstrap() override {
        NeedRedirect = false;
        Database = "/Root";
        auto board = std::make_shared<TEvStateStorage::TEvBoardInfo>(TEvStateStorage::TEvBoardInfo::EStatus::Ok, Database);
        board->InfoEntries[SelfId()] = {};
        DatabaseBoardInfoResponse.emplace(std::move(board));

        auto navigate = std::make_shared<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(new TNavigate());
        auto& entry = navigate->Request->ResultSet.emplace_back();
        entry.Status = TNavigate::EStatus::Ok;
        entry.Path = {"Root"};
        entry.Self = new TNavigate::TDirEntryInfo();
        auto description = MakeIntrusive<TNavigate::TDomainDescription>();
        description->Description.MutableDomainKey()->SetSchemeShard(AppData()->DomainsInfo->GetDomain()->SchemeRoot);
        description->Description.MutableDomainKey()->SetPathId(1);
        entry.DomainDescription = description;
        if (DomainKey) {
            entry.DomainInfo = new NSchemeCache::TDomainInfo(*DomainKey, *DomainKey);
        }
        DatabaseNavigateResponse.emplace(std::move(navigate));
        TJsonStorageStats::Bootstrap();
    }
};

void CheckTabletScope(bool strictDatabaseUser, bool useHive, std::optional<TPathId> domainKey) {
    TPortManager ports;
    auto settings = Tests::TServerSettings(ports.GetPort())
        .SetNodeCount(1)
        .SetUseRealThreads(false)
        .SetDomainName("Root")
        .SetUseSectorMap(true)
        .InitKikimrRunConfig();
    Tests::TServer server(settings);
    auto& runtime = *server.GetRuntime();
    auto& appData = runtime.GetAppData();
    appData.AdministrationAllowedSIDs = {"root"};
    auto& security = *appData.DomainsConfig.MutableSecurityConfig();
    security.AddDatabaseAllowedSIDs("database");
    security.AddViewerAllowedSIDs("viewer");
    security.AddMonitoringAllowedSIDs("monitoring");

    auto* viewer = dynamic_cast<IViewer*>(runtime.FindActor(runtime.GetLocalServiceId(MakeViewerID(0))));
    UNIT_ASSERT(viewer);
    const auto sender = runtime.AllocateEdgeActor();
    NViewerTests::THttpRequest httpRequest(HTTP_METHOD_GET);
    httpRequest.CgiParameters.emplace("database", "/Root");
    httpRequest.CgiParameters.emplace("group_by", "tablet_type");
    httpRequest.CgiParameters.emplace("use_hive_tablets", useHive ? "1" : "0");
    httpRequest.CgiParameters.emplace("debug", "true");
    TMonPage page;
    NMonitoring::TMonService2HttpRequest monRequest(nullptr, &httpRequest, nullptr, &page, "/storage_stats", nullptr);
    NACLib::TUserToken token(strictDatabaseUser ? "database" : "viewer", {});
    auto event = IEventHandle::Downcast<NMon::TEvHttpInfo>(
        new IEventHandle(sender, sender, new NMon::TEvHttpInfo(monRequest, token.SerializeAsString())));

    ui32 hiveRequests = 0;
    ui32 whiteboardRequests = 0;
    auto checkFilter = [&](const auto& filter) {
        if (domainKey) {
            UNIT_ASSERT_VALUES_EQUAL(filter.GetSchemeShard(), domainKey->OwnerId);
            UNIT_ASSERT_VALUES_EQUAL(filter.GetPathId(), domainKey->LocalPathId);
        }
    };
    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvHive::EvRequestHiveInfo) {
            ++hiveRequests;
            const auto& record = ev->Get<TEvHive::TEvRequestHiveInfo>()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.HasFilterTabletsByObjectDomain(), domainKey.has_value());
            checkFilter(record.GetFilterTabletsByObjectDomain());
        }
        if (ev->GetTypeRewrite() == TEvWhiteboard::EvTabletStateRequest) {
            ++whiteboardRequests;
            const auto& record = ev->Get<TEvWhiteboard::TEvTabletStateRequest>()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.HasFilterTenantId(), domainKey.has_value());
            checkFilter(record.GetFilterTenantId());
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });
    runtime.Register(new TStorageStatsWithNavigate(viewer, event, domainKey));
    TAutoPtr<IEventHandle> handle;
    const auto* result = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);
    if (strictDatabaseUser && (!domainKey || !domainKey->OwnerId || !domainKey->LocalPathId)) {
        UNIT_ASSERT_STRING_CONTAINS(result->Answer, "HTTP/1.1 403 ");
        UNIT_ASSERT_STRING_CONTAINS(result->Answer, "Database tablet scope is unavailable");
        UNIT_ASSERT(!result->Answer.Contains("TabletIds"));
        UNIT_ASSERT_VALUES_EQUAL(hiveRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(whiteboardRequests, 0);
    } else {
        UNIT_ASSERT_STRING_CONTAINS(result->Answer, "HTTP/1.1 200 ");
        UNIT_ASSERT_C(useHive ? hiveRequests > 0 : whiteboardRequests > 0, "Tablet information must be requested");
    }
}

} // namespace

Y_UNIT_TEST_SUITE(StorageStatsAccess) {
    // Missing DomainInfo must deny strict database user requests before either tablet source is contacted.
    Y_UNIT_TEST(StrictDatabaseUserRequestWithoutDomainInfo) {
        for (bool useHive : {false, true}) {
            CheckTabletScope(/* strictDatabaseUser */ true, useHive, std::nullopt);
        }
    }

    // A partially populated domain key is not a trustworthy tenant filter.
    Y_UNIT_TEST(StrictDatabaseUserRequestWithIncompleteDomainKey) {
        for (bool useHive : {false, true}) {
            CheckTabletScope(/* strictDatabaseUser */ true, useHive, TPathId(0, 1));
            CheckTabletScope(/* strictDatabaseUser */ true, useHive, TPathId(42, 0));
        }
    }

    // Every strict tablet request carries the resolved database key to Hive or whiteboard.
    Y_UNIT_TEST(StrictDatabaseUserRequestWithDomainKey) {
        for (bool useHive : {false, true}) {
            CheckTabletScope(/* strictDatabaseUser */ true, useHive, TPathId(42, 7));
        }
    }

    // Viewer+ retains its existing behavior when the database key is unavailable.
    Y_UNIT_TEST(ViewerUserRequestWithoutDomainInfo) {
        for (bool useHive : {false, true}) {
            CheckTabletScope(/* strictDatabaseUser */ false, useHive, std::nullopt);
        }
    }
}

} // namespace NKikimr::NViewer
