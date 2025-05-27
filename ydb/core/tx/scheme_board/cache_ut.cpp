#include "cache.h"
#include "ut_helpers.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NSchemeBoard {

using namespace NSchemeShardUT_Private;
using TConfig = NSchemeCache::TSchemeCacheConfig;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;
using TResolve = NSchemeCache::TSchemeCacheRequest;

class TCacheTest: public TTestWithSchemeshard {
public:
    void SetUp() override {
        TTestWithSchemeshard::SetUp();

        TIntrusivePtr<TConfig> config = new TConfig();
        config->Counters = new ::NMonitoring::TDynamicCounters;
        config->Roots.push_back(TConfig::TTagEntry(0, TTestTxConfig::SchemeShard, "Root"));
        SchemeCache = Context->Register(CreateSchemeBoardSchemeCache(config.Get()));
        Context->EnableScheduleForActor(SchemeCache, true);

        TestAlterSubDomain(*Context, 1, "/", R"(
            Name: "Root"
            StoragePools {
              Name: "pool-1"
              Kind: "pool-kind-1"
            }
        )");
    }

    UNIT_TEST_SUITE(TCacheTest);
    UNIT_TEST(Navigate);
    UNIT_TEST(Attributes);
    UNIT_TEST(List);
    UNIT_TEST(Recreate);
    UNIT_TEST(RacyRecreateAndSync);
    UNIT_TEST(RacyCreateAndSync);
    UNIT_TEST(CheckAccess);
    UNIT_TEST(CheckSystemViewAccess);
    UNIT_TEST(SystemView);
    UNIT_TEST(SysLocks);
    UNIT_TEST(TableSchemaVersion);
    UNIT_TEST(MigrationCommon);
    UNIT_TEST(MigrationCommit);
    UNIT_TEST(MigrationLostMessage);
    UNIT_TEST(MigrationUndo);
    UNIT_TEST(MigrationDeletedPathNavigate);
    UNIT_TEST(WatchRoot);
    UNIT_TEST(PathBelongsToDomain);
    UNIT_TEST(CookiesArePreserved);
    UNIT_TEST_SUITE_END();

    void Navigate();
    void Attributes();
    void List();
    void Recreate();
    void RacyRecreateAndSync();
    void RacyCreateAndSync();
    void CheckAccess();
    void CheckSystemViewAccess();
    void SystemView();
    void SysLocks();
    void TableSchemaVersion();
    void MigrationCommon();
    void MigrationCommit();
    void MigrationLostMessage();
    void MigrationUndo();
    void MigrationDeletedPathNavigate();
    void WatchRoot();
    void PathBelongsToDomain();
    void CookiesArePreserved();

protected:
    TNavigate::TEntry TestNavigateImpl(THolder<TNavigate> request, TNavigate::EStatus expectedStatus,
        const TString& sid, TNavigate::EOp op, bool showPrivatePath, bool redirectRequired, ui64 cookie = 0);

    TNavigate::TEntry TestNavigate(const TString& path, TNavigate::EStatus expectedStatus = TNavigate::EStatus::Ok,
        const TString& sid = TString(), TNavigate::EOp op = TNavigate::EOp::OpPath,
        bool showPrivatePath = false, bool redirectRequired = true, bool syncVersion = false);

    TNavigate::TEntry TestNavigateByTableId(const TTableId& tableId, TNavigate::EStatus expectedStatus,
        const TString& expectedPath, const TString& sid = TString(),
        TNavigate::EOp op = TNavigate::EOp::OpPath, bool showPrivatePath = false);

    TResolve::TEntry TestResolve(const TTableId& tableId, TResolve::EStatus expectedStatus = TResolve::EStatus::OkData,
        const TString& sid = TString());

    TActorId TestWatch(const TPathId& pathId, const TActorId& watcher = {}, ui64 key = 0);
    void TestWatchRemove(const TActorId& watcher, ui64 key = 0);
    NSchemeCache::TDescribeResult::TCPtr ExpectWatchUpdated(const TActorId& watcher, const TString& expectedPath = {});
    TPathId ExpectWatchDeleted(const TActorId& watcher);

    void CreateAndMigrateWithoutDecision(ui64& txId);

private:
    TActorId SchemeCache;

}; // TCacheTest

UNIT_TEST_SUITE_REGISTRATION(TCacheTest);

void TCacheTest::Navigate() {
    TestNavigate("/Root", TNavigate::EStatus::Ok);

    ui64 txId = 100;

    TestMkDir(*Context, ++txId, "/Root", "DirA");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));

    auto entry = TestNavigate("/Root/DirA", TNavigate::EStatus::Ok);

    TestNavigateByTableId(entry.TableId, TNavigate::EStatus::Ok, "/Root/DirA");
    TestNavigateByTableId(TTableId(2UL << 56, 1), TNavigate::EStatus::RootUnknown, "");
}

void TCacheTest::Attributes() {
    NKikimrSchemeOp::TAlterUserAttributes attrs;
    auto& attr = *attrs.AddUserAttributes();
    attr.SetKey("key");
    attr.SetValue("value");

    ui64 txId = 100;
    TestMkDir(*Context, ++txId, "/Root", "DirA", {NKikimrScheme::StatusAccepted}, attrs);
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));

    auto entry = TestNavigate("/Root/DirA", TNavigate::EStatus::Ok);
    UNIT_ASSERT_VALUES_EQUAL(entry.Attributes.size(), 1);
    UNIT_ASSERT(entry.Attributes.contains("key"));
    UNIT_ASSERT_VALUES_EQUAL(entry.Attributes["key"], "value");
}

void TCacheTest::List() {
    ui64 txId = 100;

    TestMkDir(*Context, ++txId, "/Root", "DirA");
    TestMkDir(*Context, ++txId, "/Root/DirA", "DirB");
    TestMkDir(*Context, ++txId, "/Root/DirA", "DirC");
    TestWaitNotification(*Context, {txId - 2, txId - 1, txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));

    {
        auto entry = TestNavigate("/Root/DirA", TNavigate::EStatus::Ok);
        UNIT_ASSERT(!entry.ListNodeEntry);
    }
    {
        auto entry = TestNavigate("/Root/DirA", TNavigate::EStatus::Ok, TString(), TNavigate::OpList);
        UNIT_ASSERT(entry.ListNodeEntry);
        UNIT_ASSERT_VALUES_EQUAL(entry.ListNodeEntry->Children.size(), 2);
    }
}

void TCacheTest::Recreate() {
    const TActorId edge = Context->AllocateEdgeActor();
    ui64 txId = 100;

    TestMkDir(*Context, ++txId, "/Root", "DirA");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));
    Context->CreateSubscriber<TSchemeBoardEvents::TEvNotifyUpdate>(edge, "/Root/DirA");

    TestRmDir(*Context, ++txId, "/Root", "DirA");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));
    auto ev = Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyDelete>(edge);
    auto pathId = ev->Get()->PathId;
    TTableId tableId(pathId.OwnerId, pathId.LocalPathId);
    TestResolve(tableId, TResolve::EStatus::PathErrorNotExist);

    TestMkDir(*Context, ++txId, "/Root", "DirA");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));
    Context->GrabEdgeEvent<TSchemeBoardEvents::TEvNotifyUpdate>(edge);
    TestNavigate("/Root/DirA", TNavigate::EStatus::Ok);
}

void TCacheTest::RacyCreateAndSync() {
    THolder<IEventHandle> delayedSyncRequest;
    auto prevObserver = Context->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
        case TSchemeBoardEvents::EvSyncRequest:
            delayedSyncRequest.Reset(ev.Release());
            return TTestActorRuntime::EEventAction::DROP;
        default:
            return TTestActorRuntime::EEventAction::PROCESS;
        }
    });

    TNavigate::TEntry entry;
    entry.Path = SplitPath("/Root/DirA");
    entry.SyncVersion = true;
    entry.Operation = TNavigate::OpPath;

    auto request = MakeHolder<TNavigate>();
    request->ResultSet.push_back(entry);

    const TActorId edge = Context->AllocateEdgeActor();
    Context->Send(SchemeCache, edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0, 0, 0, true);

    if (!delayedSyncRequest) {
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&delayedSyncRequest](IEventHandle&) -> bool {
            return bool(delayedSyncRequest);
        });
        Context->DispatchEvents(opts);
    }

    Context->SetObserverFunc(prevObserver);

    ui64 txId = 100;
    TestMkDir(*Context, ++txId, "/Root", "DirA");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));

    Context->TTestBasicRuntime::Send(delayedSyncRequest.Release(), 0, true);
    auto ev = Context->GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Request->ErrorCount, 0);
}

void TCacheTest::RacyRecreateAndSync() {
    ui64 txId = 100;

    TestMkDir(*Context, ++txId, "/Root", "DirA");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));
    TestNavigate("/Root/DirA", TNavigate::EStatus::Ok, "", TNavigate::EOp::OpPath, false, true, true);

    TestRmDir(*Context, ++txId, "/Root", "DirA");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));
    TestNavigate("/Root/DirA", TNavigate::EStatus::PathErrorUnknown, "", TNavigate::EOp::OpPath, false, true, true);

    THolder<IEventHandle> delayedSyncRequest;
    auto prevObserver = Context->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
        case TSchemeBoardEvents::EvSyncRequest:
            delayedSyncRequest.Reset(ev.Release());
            return TTestActorRuntime::EEventAction::DROP;
        default:
            return TTestActorRuntime::EEventAction::PROCESS;
        }
    });

    TNavigate::TEntry entry;
    entry.Path = SplitPath("/Root/DirA");
    entry.SyncVersion = true;
    entry.Operation = TNavigate::OpPath;

    auto request = MakeHolder<TNavigate>();
    request->ResultSet.push_back(entry);

    const TActorId edge = Context->AllocateEdgeActor();
    Context->Send(SchemeCache, edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0, 0, 0, true);

    if (!delayedSyncRequest) {
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&delayedSyncRequest](IEventHandle&) -> bool {
            return bool(delayedSyncRequest);
        });
        Context->DispatchEvents(opts);
    }

    Context->SetObserverFunc(prevObserver);

    TestMkDir(*Context, ++txId, "/Root", "DirA");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));

    Context->TTestBasicRuntime::Send(delayedSyncRequest.Release(), 0, true);
    auto ev = Context->GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT(!ev->Get()->Request->ResultSet.empty());
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Request->ErrorCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Request->ResultSet.at(0).Status, TNavigate::EStatus::Ok);
}

void TCacheTest::CheckAccess() {
    ui64 txId = 100;
    TestMkDir(*Context, ++txId, "/Root", "DirA");
    TestModifyACL(*Context, ++txId, "/Root", "DirA", TString(), "user0@builtin");

    auto entry = TestNavigate("/Root/DirA", TNavigate::EStatus::Ok);

    auto byPath = TestNavigate("/Root/DirA", TNavigate::EStatus::PathErrorUnknown, "user1@builtin");
    UNIT_ASSERT_VALUES_EQUAL(CanonizePath(byPath.Path), "/Root/DirA");
    UNIT_ASSERT_VALUES_EQUAL(byPath.TableId, TTableId());
    TestNavigate("/Root/DirA", TNavigate::EStatus::Ok, "user0@builtin");

    auto byTableId = TestNavigateByTableId(entry.TableId, TNavigate::EStatus::PathErrorUnknown, "", "user1@builtin");
    UNIT_ASSERT_VALUES_EQUAL(CanonizePath(byTableId.Path), "");
    UNIT_ASSERT_VALUES_EQUAL(byTableId.TableId, entry.TableId);
    TestNavigateByTableId(entry.TableId, TNavigate::EStatus::Ok, "/Root/DirA", "user0@builtin");
}

void TCacheTest::CheckSystemViewAccess() {
    ui64 txId = 100;
    TestCreateSubDomain(*Context, ++txId, "/Root", "Name: \"SubDomainA\"");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));
    TestModifyACL(*Context, ++txId, "/Root", "SubDomainA", TString(), "user0@builtin");

    auto entry = TestNavigate("/Root/SubDomainA/.sys/partition_stats",
        TNavigate::EStatus::Ok, TString(), TNavigate::OpTable);

    auto tableId = entry.TableId;
    UNIT_ASSERT_VALUES_EQUAL(tableId.SysViewInfo, "partition_stats");

    TestNavigate("/Root/SubDomainA/.sys/partition_stats",
        TNavigate::EStatus::Ok, "user0@builtin", TNavigate::OpTable);

    TestNavigate("/Root/SubDomainA/.sys/partition_stats",
        TNavigate::EStatus::PathErrorUnknown, "user1@builtin", TNavigate::OpTable);

    TestResolve(tableId, TResolve::EStatus::OkData);
    TestResolve(tableId, TResolve::EStatus::OkData, "user0@builtin");
    TestResolve(tableId, TResolve::EStatus::PathErrorNotExist, "user1@builtin");
}

void TCacheTest::SystemView() {
    auto entry = TestNavigate("/Root/.sys/partition_stats", TNavigate::EStatus::Ok, TString(), TNavigate::OpTable);

    auto tableId = entry.TableId;
    UNIT_ASSERT_VALUES_EQUAL(tableId.SysViewInfo, "partition_stats");

    TestNavigateByTableId(tableId, TNavigate::EStatus::Ok, "/Root/.sys/partition_stats", TString(), TNavigate::OpTable);
}

void TCacheTest::SysLocks() {
    {
        auto entry = TestNavigate("/sys/locks", TNavigate::EStatus::Ok, TString(), TNavigate::OpTable, true);
        TestNavigateByTableId(entry.TableId, TNavigate::EStatus::Ok, "/sys/locks", TString(), TNavigate::OpTable, true);
    }
    {
        auto entry = TestNavigate("/sys/locks2", TNavigate::EStatus::Ok, TString(), TNavigate::OpTable, true);
        TestNavigateByTableId(entry.TableId, TNavigate::EStatus::Ok, "/sys/locks2", TString(), TNavigate::OpTable, true);
    }
}

void TCacheTest::TableSchemaVersion() {
    ui64 txId = 100;
    TestCreateTable(*Context, ++txId, "/Root", R"(
        Name: "Table1"
        Columns { Name: "key" Type: "Uint32" }
        KeyColumnNames: [ "key" ]
        PartitionConfig {
            CompactionPolicy {
            }
        }
    )", {NKikimrScheme::StatusAccepted});

    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));
    {
        auto entry = TestNavigate("/Root/Table1", TNavigate::EStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(entry.TableId.SchemaVersion, 1);
    }

    TestAlterTable(*Context, ++txId, "/Root", R"(
        Name: "Table1"
        Columns { Name: "added"  Type: "Uint64"}
    )");

    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));
    {
        auto entry = TestNavigate("/Root/Table1", TNavigate::EStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(entry.TableId.SchemaVersion, 2);
    }
}

TNavigate::TEntry TCacheTest::TestNavigateImpl(THolder<TNavigate> request, TNavigate::EStatus expectedStatus,
    const TString& sid, TNavigate::EOp op, bool showPrivatePath, bool redirectRequired, ui64 cookie)
{
    auto& entry = request->ResultSet.back();
    entry.Operation = op;
    entry.ShowPrivatePath = showPrivatePath;
    entry.RedirectRequired = redirectRequired;

    if (sid) {
        request->UserToken = new NACLib::TUserToken(sid, {});
    }

    const TActorId edge = Context->AllocateEdgeActor();
    Context->Send(SchemeCache, edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0, cookie, 0, true);
    auto ev = Context->GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, cookie);
    UNIT_ASSERT(!ev->Get()->Request->ResultSet.empty());

    const TNavigate::TEntry result = ev->Get()->Request->ResultSet[0];
    UNIT_ASSERT_VALUES_EQUAL(result.Status, expectedStatus);
    return result;
}

TNavigate::TEntry TCacheTest::TestNavigate(const TString& path, TNavigate::EStatus expectedStatus,
    const TString& sid, TNavigate::EOp op, bool showPrivatePath,  bool redirectRequired, bool syncVersion)
{
    auto request = MakeHolder<TNavigate>();
    request->ResultSet.push_back({});
    auto& entry = request->ResultSet.back();

    entry.Path = SplitPath(path);
    entry.SyncVersion = syncVersion;

    auto result = TestNavigateImpl(std::move(request), expectedStatus, sid, op, showPrivatePath, redirectRequired);
    return result;
}

TNavigate::TEntry TCacheTest::TestNavigateByTableId(const TTableId& tableId, TNavigate::EStatus expectedStatus,
    const TString& expectedPath, const TString& sid, TNavigate::EOp op, bool showPrivatePath)
{
    auto request = MakeHolder<TNavigate>();
    request->ResultSet.push_back({});
    auto& entry = request->ResultSet.back();

    entry.TableId = tableId;
    entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;

    auto result = TestNavigateImpl(std::move(request), expectedStatus, sid, op, showPrivatePath, true);
    UNIT_ASSERT_VALUES_EQUAL(CanonizePath(result.Path), expectedPath);
    return result;
}

TResolve::TEntry TCacheTest::TestResolve(const TTableId& tableId, TResolve::EStatus expectedStatus, const TString& sid) {
    auto request = MakeHolder<TResolve>();

    auto keyDesc = MakeHolder<TKeyDesc>(
        tableId,
        TTableRange({}),
        TKeyDesc::ERowOperation::Unknown,
        TVector<NScheme::TTypeInfo>(), TVector<TKeyDesc::TColumnOp>()
    );
    request->ResultSet.emplace_back(std::move(keyDesc));

    if (sid) {
        request->UserToken = new NACLib::TUserToken(sid, {});
    }

    const TActorId edge = Context->AllocateEdgeActor();
    Context->Send(SchemeCache, edge, new TEvTxProxySchemeCache::TEvResolveKeySet(request.Release()), 0, 0, 0, true);
    auto ev = Context->GrabEdgeEvent<TEvTxProxySchemeCache::TEvResolveKeySetResult>(edge);

    UNIT_ASSERT(ev->Get());
    UNIT_ASSERT(!ev->Get()->Request->ResultSet.empty());

    TResolve::TEntry result = std::move(ev->Get()->Request->ResultSet[0]);
    UNIT_ASSERT_VALUES_EQUAL(result.Status, expectedStatus);
    return result;
}

TActorId TCacheTest::TestWatch(const TPathId& pathId, const TActorId& watcher, ui64 key) {
    const TActorId edge = watcher ? watcher : Context->AllocateEdgeActor();
    Context->Send(SchemeCache, edge, new TEvTxProxySchemeCache::TEvWatchPathId(pathId, key), 0, 0, 0, true);
    return edge;
}

void TCacheTest::TestWatchRemove(const TActorId& watcher, ui64 key) {
    Context->Send(SchemeCache, watcher, new TEvTxProxySchemeCache::TEvWatchRemove(key), 0, 0, 0, true);
}

NSchemeCache::TDescribeResult::TCPtr TCacheTest::ExpectWatchUpdated(const TActorId& watcher, const TString& expectedPath) {
    auto ev = Context->GrabEdgeEvent<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(watcher);
    if (expectedPath) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Path, expectedPath);
    }
    return ev->Get()->Result;
}

TPathId TCacheTest::ExpectWatchDeleted(const TActorId& watcher) {
    auto ev = Context->GrabEdgeEvent<TEvTxProxySchemeCache::TEvWatchNotifyDeleted>(watcher);
    return ev->Get()->PathId;
}

void TCacheTest::CreateAndMigrateWithoutDecision(ui64& txId) {
    auto domainSSNotifier = CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard);

    TestCreateSubDomain(*Context, ++txId,  "/Root",
                        "Name: \"USER_0\"");
    TestAlterSubDomain(*Context, ++txId,  "/Root",
                       "Name: \"USER_0\" "
                       "PlanResolution: 50 "
                       "Coordinators: 1 "
                       "Mediators: 1 "
                       "TimeCastBucketsPerMediator: 2 ");
    TestWaitNotification(*Context, {txId, txId - 1}, domainSSNotifier);

    TestMkDir(*Context, ++txId, "/Root/USER_0", "DirA");
    TestCreateTable(*Context, ++txId, "/Root/USER_0/DirA", R"(
        Name: "Table1"
        Columns { Name: "key" Type: "Uint32" }
        KeyColumnNames: [ "key" ]
        PartitionConfig {
            CompactionPolicy {
            }
        }
    )");

    TestWaitNotification(*Context, {txId, txId - 1}, domainSSNotifier);

    {
        auto entry = TestNavigate("/Root/USER_0", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0");
        UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 2));
        UNIT_ASSERT(!entry.DomainInfo->Params.HasSchemeShard());
    }

    {
        auto entry = TestNavigate("/Root/USER_0/DirA", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA");
        UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 3));
        UNIT_ASSERT(!entry.DomainInfo->Params.HasSchemeShard());
    }
    {
        auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
        UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
        UNIT_ASSERT(!entry.DomainInfo->Params.HasSchemeShard());
    }

    TestUpgradeSubDomain(*Context, ++txId,  "/Root", "USER_0");

    TestWaitNotification(*Context, {txId}, domainSSNotifier);
}

void TCacheTest::MigrationCommon() {
    Context->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    Context->SetLogPriority(NKikimrServices::SCHEMESHARD_DESCRIBE, NActors::NLog::PRI_TRACE);
    Context->SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_TRACE);

    TurnOnTabletsScheduling();

    ui64 txId = 100;

    CreateAndMigrateWithoutDecision(txId);

    auto checkMigratedPathes = [&] () {
        {
            auto entry = TestNavigate("/Root/USER_0", TNavigate::EStatus::Ok);
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0");
            UNIT_ASSERT_UNEQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 2));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(entry.DomainInfo->Params.GetSchemeShard(), 1));
        }
        {
            auto entry = TestNavigate("/Root/USER_0/DirA", TNavigate::EStatus::Ok);
            Cerr << entry.ToString() << Endl;
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA");
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 3));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
        }
        {
            auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
        }
    };

    checkMigratedPathes();

    // global ss do not wipe migrated pathes after restart
    RebootTablet(*Context, (ui64)TTestTxConfig::SchemeShard, Context->AllocateEdgeActor());

    checkMigratedPathes();
}

void TCacheTest::MigrationCommit() {
    TurnOnTabletsScheduling();

    ui64 txId = 100;

    CreateAndMigrateWithoutDecision(txId);

    auto checkMigratedPathes = [&] () {
        {
            auto entry = TestNavigate("/Root/USER_0", TNavigate::EStatus::Ok);
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0");
            UNIT_ASSERT_UNEQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 2));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(entry.DomainInfo->Params.GetSchemeShard(), 1));
        }
        {
            auto entry = TestNavigate("/Root/USER_0/DirA", TNavigate::EStatus::Ok);
            Cerr << entry.ToString() << Endl;
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA");
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 3));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
        }
        {
            auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
        }
    };

    checkMigratedPathes();

    TestUpgradeSubDomainDecision(*Context, ++txId,  "/Root", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);

    auto domainSSNotifier = CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard);
    TestWaitNotification(*Context, {txId}, domainSSNotifier);

    checkMigratedPathes();

    RebootTablet(*Context, (ui64)TTestTxConfig::SchemeShard, Context->AllocateEdgeActor());

    checkMigratedPathes();
}

void TCacheTest::MigrationLostMessage() {
    TurnOnTabletsScheduling();

    ui64 txId = 100;

    CreateAndMigrateWithoutDecision(txId);

    auto checkMigratedPathes = [&] () {
        {
            auto entry = TestNavigate("/Root/USER_0", TNavigate::EStatus::Ok);
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0");
            UNIT_ASSERT_UNEQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 2));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(entry.DomainInfo->Params.GetSchemeShard(), 1));
        }
        {
            auto entry = TestNavigate("/Root/USER_0/DirA", TNavigate::EStatus::Ok);
            Cerr << entry.ToString() << Endl;
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA");
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 3));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
        }
        {
            auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
        }
    };

    checkMigratedPathes();

    TestUpgradeSubDomainDecision(*Context, ++txId,  "/Root", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);

    auto domainSSNotifier = CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard);
    TestWaitNotification(*Context, {txId}, domainSSNotifier);

    checkMigratedPathes();

    ui64 tentantSchemeShard = 0;
    TPathId oldPathId;
    {
        auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
        UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
        UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());

        tentantSchemeShard = entry.DomainInfo->Params.GetSchemeShard();
        oldPathId = entry.TableId.PathId;
    }

    auto tenantSSNotifier = CreateNotificationSubscriber(*Context, tentantSchemeShard);

    bool deleteMsgHasBeenDropped = false;
    auto skipDeleteNotification = [&deleteMsgHasBeenDropped](TAutoPtr<IEventHandle>& ev) -> auto {
        if (ev->Type == TSchemeBoardEvents::EvNotifyDelete) {
            auto *msg = ev->Get<TSchemeBoardEvents::TEvNotifyDelete>();
            Cerr << Endl << Endl << Endl << "skipDeleteNotification"
                 << " path: " << msg->Path
                 << " pathId: " << msg->PathId
                 << " Strong: " << msg->Strong
                 << Endl << Endl << Endl;
            deleteMsgHasBeenDropped = true;
            return TTestActorRuntime::EEventAction::DROP;

        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };

    auto prevObserverFunc = Context->SetObserverFunc(skipDeleteNotification);

    TestDropTable(*Context, tentantSchemeShard, ++txId, "/Root/USER_0/DirA", "Table1");
    TestWaitNotification(*Context, {txId}, tenantSSNotifier);

    UNIT_ASSERT_EQUAL(deleteMsgHasBeenDropped, true);
    deleteMsgHasBeenDropped = false;

    { // hang in chache
        auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
        UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
        UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
    }

    TestRmDir(*Context, tentantSchemeShard, ++txId, "/Root/USER_0", "DirA");
    TestWaitNotification(*Context, {txId}, tenantSSNotifier);

    { // hang in chache
        auto entry = TestNavigate("/Root/USER_0/DirA", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA");
        UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 3));
        UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
    }

    UNIT_ASSERT_EQUAL(deleteMsgHasBeenDropped, true);
    deleteMsgHasBeenDropped = false;

    TestMkDir(*Context, tentantSchemeShard, ++txId, "/Root/USER_0", "DirA");
    TestWaitNotification(*Context, {txId}, tenantSSNotifier);

    {
        auto entry = TestNavigate("/Root/USER_0/DirA", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA");
        UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(tentantSchemeShard, 2));
        UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
    }

    {
        TestNavigateByTableId(TTableId(TTestTxConfig::SchemeShard, 3), TNavigate::EStatus::Ok, "/Root/USER_0/DirA");
        TestNavigateByTableId(TTableId(tentantSchemeShard, 2), TNavigate::EStatus::Ok, "/Root/USER_0/DirA");
    }

    Context->SetObserverFunc(prevObserverFunc);

    { // hang in chache still
        auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
        UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
        UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
    }

    TestCreateTable(*Context, tentantSchemeShard, ++txId, "/Root/USER_0/DirA", R"(
        Name: "Table1"
        Columns { Name: "key" Type: "Uint64" }
        KeyColumnNames: [ "key" ]
    )");
    TestWaitNotification(*Context, {txId}, tenantSSNotifier);

    {
        auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
        UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(tentantSchemeShard, 3));
        UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());

        TestNavigateByTableId(TTableId(tentantSchemeShard, 3), TNavigate::EStatus::Ok, "/Root/USER_0/DirA/Table1");
    }
}

void TCacheTest::MigrationUndo() {
    Context->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    Context->SetLogPriority(NKikimrServices::SCHEMESHARD_DESCRIBE, NActors::NLog::PRI_TRACE);
    //Context->SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_TRACE);

    //Context->SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NActors::NLog::PRI_TRACE);
    //Context->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_TRACE);
    Context->SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NActors::NLog::PRI_TRACE);
    Context->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_TRACE);


    TurnOnTabletsScheduling();

    ui64 txId = 100;

    CreateAndMigrateWithoutDecision(txId);

    auto checkMigratedPathes = [&] () {
        {
            auto entry = TestNavigate("/Root/USER_0", TNavigate::EStatus::Ok);
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0");
            UNIT_ASSERT_UNEQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 2));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(entry.DomainInfo->Params.GetSchemeShard(), 1));
        }
        {
            auto entry = TestNavigate("/Root/USER_0/DirA", TNavigate::EStatus::Ok);
            Cerr << entry.ToString() << Endl;
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA");
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 3));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
        }
        {
            auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
            UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());
        }
    };

    checkMigratedPathes();

    TestUpgradeSubDomainDecision(*Context, ++txId,  "/Root", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Undo);

    auto domainSSNotifier = CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard);
    TestWaitNotification(*Context, {txId}, domainSSNotifier);

    auto checkRevertedPathes = [&] () {
        {
            auto entry = TestNavigate("/Root/USER_0", TNavigate::EStatus::Ok);
            Cerr << entry.ToString() << Endl;
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0");
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 2));
            UNIT_ASSERT(!entry.DomainInfo->Params.HasSchemeShard());
        }
        {
            auto entry = TestNavigate("/Root/USER_0/DirA", TNavigate::EStatus::Ok);
            Cerr << entry.ToString() << Endl;
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA");
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 3));
            UNIT_ASSERT(!entry.DomainInfo->Params.HasSchemeShard());
        }
        {
            auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
            UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
            UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
            UNIT_ASSERT(!entry.DomainInfo->Params.HasSchemeShard());
        }
    };


    checkRevertedPathes();

    RebootTablet(*Context, (ui64)TTestTxConfig::SchemeShard, Context->AllocateEdgeActor());

    checkRevertedPathes();
}

void SimulateSleep(TTestContext& context, TDuration duration) {
    auto sender = context.AllocateEdgeActor();
    context.Schedule(new IEventHandle(sender, sender, new TEvents::TEvWakeup()), duration);
    context.GrabEdgeEventRethrow<TEvents::TEvWakeup>(sender);
}

void TCacheTest::MigrationDeletedPathNavigate() {
    TurnOnTabletsScheduling();

    ui64 txId = 100;

    CreateAndMigrateWithoutDecision(txId);

    TestUpgradeSubDomainDecision(*Context, ++txId,  "/Root", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);

    auto domainSSNotifier = CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard);
    TestWaitNotification(*Context, {txId}, domainSSNotifier);

    ui64 tentantSchemeShard = 0;
    TPathId oldPathId;
    {
        auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
        UNIT_ASSERT_EQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
        UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());

        tentantSchemeShard = entry.DomainInfo->Params.GetSchemeShard();
        oldPathId = entry.TableId.PathId;
    }

    auto tenantSSNotifier = CreateNotificationSubscriber(*Context, tentantSchemeShard);

    TestDropTable(*Context, tentantSchemeShard, ++txId, "/Root/USER_0/DirA", "Table1");
    TestWaitNotification(*Context, {txId}, tenantSSNotifier);

    TestCreateTable(*Context, tentantSchemeShard, ++txId, "/Root/USER_0/DirA", R"(
        Name: "Table1"
        Columns { Name: "key" Type: "Uint32" }
        KeyColumnNames: [ "key" ]
        PartitionConfig {
            CompactionPolicy {
            }
        }
    )");
    TestWaitNotification(*Context, {txId}, tenantSSNotifier);

    TPathId newPathId;
    {
        auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
        UNIT_ASSERT_UNEQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
        UNIT_ASSERT(entry.DomainInfo->Params.HasSchemeShard());

        newPathId = entry.TableId.PathId;
    }

    for (int i = 0; i < 6; ++i) {
        SimulateSleep(*Context, TDuration::Seconds(30));

        auto entry = TestNavigate("/Root/USER_0/DirA/Table1", TNavigate::EStatus::Ok);
        UNIT_ASSERT_EQUAL(JoinPath(entry.Path), "Root/USER_0/DirA/Table1");
        UNIT_ASSERT_UNEQUAL(entry.TableId.PathId, TPathId(TTestTxConfig::SchemeShard, 4));
    }

    {
        TestResolve(TTableId(oldPathId.OwnerId, oldPathId.LocalPathId), TResolve::EStatus::PathErrorNotExist);
    }
}

void TCacheTest::WatchRoot() {
    auto watcher = TestWatch(TPathId(TTestTxConfig::SchemeShard, 1));

    {
        auto result = ExpectWatchUpdated(watcher, "/Root");
        UNIT_ASSERT_VALUES_EQUAL(result->GetStatus(), NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(result->GetPathDescription().GetSelf().GetPathId(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result->GetPathDescription().GetChildren().size(), 0u);
    }

    ui64 txId = 100;

    TestMkDir(*Context, ++txId, "/Root", "DirA");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));

    // Ignore notification before create finished
    ExpectWatchUpdated(watcher);

    ui64 dirPathId;
    {
        auto result = ExpectWatchUpdated(watcher);
        UNIT_ASSERT_VALUES_EQUAL(result->GetStatus(), NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(result->GetPathDescription().GetSelf().GetPathId(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result->GetPathDescription().GetChildren().size(), 1u);
        dirPathId = result->GetPathDescription().GetChildren(0).GetPathId();
    }

    TestWatch(TPathId(TTestTxConfig::SchemeShard, dirPathId), watcher);

    {
        auto result = ExpectWatchUpdated(watcher, "/Root/DirA");
        UNIT_ASSERT_VALUES_EQUAL(result->GetStatus(), NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(result->GetPathDescription().GetSelf().GetPathId(), dirPathId);
    }

    TestRmDir(*Context, ++txId, "/Root", "DirA");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));

    {
        auto deleted = ExpectWatchDeleted(watcher);
        UNIT_ASSERT_VALUES_EQUAL(deleted.LocalPathId, dirPathId);
    }

    // Ignore notification before drop finished
    ExpectWatchUpdated(watcher);

    {
        auto result = ExpectWatchUpdated(watcher);
        UNIT_ASSERT_VALUES_EQUAL(result->GetStatus(), NKikimrScheme::StatusSuccess);
        UNIT_ASSERT_VALUES_EQUAL(result->GetPathDescription().GetSelf().GetPathId(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result->GetPathDescription().GetChildren().size(), 0u);
    }

    TestWatchRemove(watcher);
    SimulateSleep(*Context, TDuration::Seconds(1));
}

void TCacheTest::PathBelongsToDomain() {
    ui64 txId = 100;
    TestCreateSubDomain(*Context, ++txId, "/Root", "Name: \"SubDomain\"");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));
    TestMkDir(*Context, ++txId, "/Root/SubDomain", "DirA");

    TTableId testId;

    // ok
    {
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = "/Root/SubDomain";
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath("/Root/SubDomain/DirA");
        entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
        auto result  = TestNavigateImpl(std::move(request), TNavigate::EStatus::Ok,
            "", TNavigate::EOp::OpPath, false, true);

        testId = result.TableId;
    }
    // error, by path
    {
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = "/Root";
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath("/Root/SubDomain/DirA");
        entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
        auto result  = TestNavigateImpl(std::move(request), TNavigate::EStatus::PathErrorUnknown,
            "", TNavigate::EOp::OpPath, false, true);
    }
    // error, by path id
    {
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = "/Root";
        auto& entry = request->ResultSet.emplace_back();
        entry.TableId = testId;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        auto result  = TestNavigateImpl(std::move(request), TNavigate::EStatus::PathErrorUnknown,
            "", TNavigate::EOp::OpPath, false, true);
    }
}

void TCacheTest::CookiesArePreserved() {
    ui64 txId = 100;
    TestCreateSubDomain(*Context, ++txId, "/Root", R"(Name: "SubDomain")");
    TestWaitNotification(*Context, {txId}, CreateNotificationSubscriber(*Context, TTestTxConfig::SchemeShard));
    TestMkDir(*Context, ++txId, "/Root/SubDomain", "DirA");

    ui64 cookie = 1;
    // first request will run db resolver
    for (int i = 0; i < 2; ++i) {
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = "/Root/SubDomain";
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath("/Root/SubDomain/DirA");
        entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
        auto result  = TestNavigateImpl(std::move(request), TNavigate::EStatus::Ok,
            "", TNavigate::EOp::OpPath, false, true, ++cookie);
    }
}

class TCacheTestWithDrops: public TCacheTest {
public:
    TTestContext::TEventObserver ObserverFunc() override {
        return [](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TSchemeBoardEvents::EvNotifyUpdate:
            case TSchemeBoardEvents::EvNotifyDelete:
            case TSchemeBoardEvents::EvSyncResponse:
                return TTestContext::EEventAction::DROP;
            }

            return TTestContext::EEventAction::PROCESS;
        };
    }

    UNIT_TEST_SUITE(TCacheTestWithDrops);
    UNIT_TEST(LookupErrorUponEviction);
    UNIT_TEST_SUITE_END();

    void LookupErrorUponEviction();

}; // TCacheTestWithDrops

UNIT_TEST_SUITE_REGISTRATION(TCacheTestWithDrops);

void TCacheTestWithDrops::LookupErrorUponEviction() {
    TestNavigate("/Root/with_sync", TNavigate::EStatus::LookupError, "", TNavigate::EOp::OpPath, false, true, true);
    TestNavigate("/Root/without_sync", TNavigate::EStatus::LookupError, "", TNavigate::EOp::OpPath, false, true, false);
}

} // NSchemeBoard
} // NKikimr
