#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <util/datetime/base.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

namespace {

std::optional<NSchemeCache::TSchemeCacheNavigate::EKind> TableKind(TServer& server, const TString& path) {
    auto* runtime = server.GetRuntime();
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
    request->ResultSet.emplace_back(std::move(entry));
    const TActorId edge = runtime->AllocateEdgeActor();
    runtime->Send(new IEventHandle(MakeSchemeCacheID(), edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())), 0, true);
    auto ev = runtime->GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);
    const auto* navigate = ev->Get()->Request.Get();
    if (!navigate || navigate->ResultSet.size() != 1
        || navigate->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok)
    {
        return std::nullopt;
    }
    return navigate->ResultSet[0].Kind;
}

bool HasColumn(TServer& server, const TString& path, const TString& name) {
    auto* runtime = server.GetRuntime();
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
    request->ResultSet.emplace_back(std::move(entry));
    const TActorId edge = runtime->AllocateEdgeActor();
    runtime->Send(new IEventHandle(MakeSchemeCacheID(), edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())), 0, true);
    auto ev = runtime->GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);
    const auto* navigate = ev->Get()->Request.Get();
    if (!navigate || navigate->ResultSet.size() != 1
        || navigate->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok)
    {
        return false;
    }
    for (const auto& [id, column] : navigate->ResultSet[0].Columns) {
        Y_UNUSED(id);
        if (column.Name == name) {
            return true;
        }
    }
    return false;
}

std::optional<NSchemeCache::TSchemeCacheNavigate::EKind> WaitKind(TServer& server, const TString& path) {
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
    do {
        if (auto kind = TableKind(server, path)) {
            return kind;
        }
        Sleep(TDuration::MilliSeconds(50));
    } while (TInstant::Now() < deadline);
    return std::nullopt;
}

} // namespace

Y_UNIT_TEST_SUITE(ColumnTable) {
    Y_UNIT_TEST(StoreColumnCreatesAColumnTable) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        const TString database = "/" + settings.DomainName;
        TSqlClient sql(server, database);

        const auto created = sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text) WITH (store = column)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);
        const auto kind = WaitKind(server, database + "/items");
        UNIT_ASSERT(kind);
        UNIT_ASSERT_VALUES_EQUAL(*kind, NSchemeCache::TSchemeCacheNavigate::KindColumnTable);

        const auto added = sql.Exec("ALTER TABLE items ADD COLUMN extra int8");
        UNIT_ASSERT_C(added.Ok, added.SqlState + " " + added.Message);
        UNIT_ASSERT(HasColumn(server, database + "/items", "extra"));
        const auto dropped = sql.Exec("ALTER TABLE items DROP COLUMN extra");
        UNIT_ASSERT_C(dropped.Ok, dropped.SqlState + " " + dropped.Message);
        UNIT_ASSERT(!HasColumn(server, database + "/items", "extra"));

        const auto other = sql.Exec("CREATE TABLE plain (id int8 PRIMARY KEY)");
        UNIT_ASSERT_C(other.Ok, other.SqlState + " " + other.Message);
        const auto row = WaitKind(server, database + "/plain");
        UNIT_ASSERT(row);
        UNIT_ASSERT_VALUES_EQUAL(*row, NSchemeCache::TSchemeCacheNavigate::KindTable);

        const auto rejected = sql.Exec("CREATE TABLE skipped (id int8 PRIMARY KEY) WITH (fillfactor = 10)");
        UNIT_ASSERT(!rejected.Ok);
        UNIT_ASSERT_VALUES_EQUAL(rejected.SqlState, "0A000");
        UNIT_ASSERT(!TableKind(server, database + "/skipped"));

        const auto floated = sql.Exec("CREATE TABLE measured (id float4 PRIMARY KEY) WITH (store = column)");
        UNIT_ASSERT(!floated.Ok);
        UNIT_ASSERT(!TableKind(server, database + "/measured"));
    }
}
