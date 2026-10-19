#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

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

struct TColumnTable {
    TTableId TableId;
    ui64 TabletId = 0;
    TVector<ui32> ColumnIds;
    TVector<TString> Names;
};

std::optional<TColumnTable> DescribeColumn(TServer& server, const TString& path) {
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
        || navigate->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
        || !navigate->ResultSet[0].ColumnTableInfo
        || navigate->ResultSet[0].ColumnTableInfo->Description.GetSharding().ColumnShardsSize() == 0)
    {
        return std::nullopt;
    }
    TColumnTable table;
    table.TableId = navigate->ResultSet[0].TableId;
    table.TabletId = navigate->ResultSet[0].ColumnTableInfo->Description.GetSharding().GetColumnShards(0);
    TVector<const TSysTables::TTableColumnInfo*> ordered;
    for (const auto& [id, column] : navigate->ResultSet[0].Columns) {
        Y_UNUSED(id);
        ordered.push_back(&column);
    }
    std::sort(ordered.begin(), ordered.end(), [](const auto* left, const auto* right) {
        return left->Id < right->Id;
    });
    for (const auto* column : ordered) {
        table.ColumnIds.push_back(column->Id);
        table.Names.push_back(column->Name);
    }
    return table;
}

bool WriteRows(TServer& server, const TColumnTable& table, const TVector<std::pair<i64, i64>>& rows) {
    auto id = std::make_shared<arrow::Int64Builder>();
    auto qty = std::make_shared<arrow::Int64Builder>();
    for (const auto& row : rows) {
        UNIT_ASSERT(id->Append(row.first).ok());
        UNIT_ASSERT(qty->Append(row.second).ok());
    }
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("qty", arrow::int64()),
    });
    auto batch = arrow::RecordBatch::Make(schema, static_cast<i64>(rows.size()), {
        id->Finish().ValueOrDie(),
        qty->Finish().ValueOrDie(),
    });
    auto write = std::make_unique<NEvents::TDataEvents::TEvWrite>(1, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
    const std::vector<ui32> columnIds(table.ColumnIds.begin(), table.ColumnIds.end());
    auto& operation = write->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT, table.TableId, columnIds, 0, NKikimrDataEvents::FORMAT_ARROW);
    *operation.MutablePayloadSchema() = NArrow::SerializeSchema(*schema);
    NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite> writer(*write);
    writer.AddDataToPayload(NArrow::SerializeBatchNoCompression(batch));
    auto* runtime = server.GetRuntime();
    const TActorId edge = runtime->AllocateEdgeActor();
    runtime->Send(new IEventHandle(MakePipePerNodeCacheID(false), edge, new TEvPipeCache::TEvForward(write.release(), table.TabletId, false)), 0, true);
    auto ev = runtime->GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(edge);
    return ev && ev->Get() && ev->Get()->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED;
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

    Y_UNIT_TEST(KeyBoundAndProgramFilter) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        const TString database = "/" + settings.DomainName;
        TSqlClient sql(server, database);

        const auto created = sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, qty int8) WITH (store = column)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);
        std::optional<TColumnTable> table;
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
        do {
            table = DescribeColumn(server, database + "/items");
            if (table) {
                break;
            }
            Sleep(TDuration::MilliSeconds(50));
        } while (TInstant::Now() < deadline);
        UNIT_ASSERT(table);
        UNIT_ASSERT(WriteRows(server, *table, {{1, 7}, {2, 9}}));

        const auto bound = sql.Exec("SELECT qty FROM items WHERE id >= 2");
        UNIT_ASSERT_C(bound.Ok, bound.SqlState + " " + bound.Message);
        UNIT_ASSERT_VALUES_EQUAL(bound.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(bound.Rows[0].size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(bound.Rows[0][0].Text, "9");
        UNIT_ASSERT(bound.AccessPath.Contains("Ranges"));
        UNIT_ASSERT(bound.AccessPath.Contains("Shards 1"));

        const auto filtered = sql.Exec("SELECT id FROM items WHERE qty = 7");
        UNIT_ASSERT_C(filtered.Ok, filtered.SqlState + " " + filtered.Message);
        UNIT_ASSERT_VALUES_EQUAL(filtered.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(filtered.Rows[0][0].Text, "1");
        UNIT_ASSERT(filtered.AccessPath.Contains("ProgramRows 1"));
    }
}
