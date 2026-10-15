#include "defs.h"
#include "ut_helpers.h"

#include <ydb/core/tx/datashard/build_index/table_range_reader.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/testlib/test_client.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDataShard::NBuildIndex {
    extern ui64 gTableRangeReaderMaxRows;
}

namespace NKikimr {

using namespace NKikimr::NDataShard::NBuildIndex;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

namespace {

const TString kReaderTable = "/Root/table-reader";

void InitReaderTable(Tests::TServer::TPtr server, TActorId sender) {
    InitRoot(server, sender);

    TShardedTableOptions options;
    options.Shards(1);
    options.Columns({
        {"key", "Uint32", true, true},
        {"value", "String", false, false},
    });
    CreateShardedTable(server, sender, "/Root", "table-reader", options);

    ExecSQL(server, sender, "UPSERT INTO `/Root/table-reader` (key, value) VALUES "
        "(1, \"one\"), (2, \"two\"), (3, \"three\"), (4, \"four\"), (5, \"five\");");
}

THolder<TEvReadTableRangeRequest> MakeReadRequest(ui64 requestId, TMaybe<ui32> from, TMaybe<ui32> to) {
    auto request = MakeHolder<TEvReadTableRangeRequest>();
    request->RequestId = requestId;
    request->DatabaseName = "/Root";
    request->TablePath = kReaderTable;
    request->Columns = {"key", "value"};

    if (from) {
        TVector<TCell> cells;
        cells.emplace_back(TCell::Make<ui32>(*from));
        request->FromKey = TSerializedCellVec(cells);
        request->FromInclusive = true;
    }
    if (to) {
        TVector<TCell> cells;
        cells.emplace_back(TCell::Make<ui32>(*to));
        request->ToKey = TSerializedCellVec(cells);
        request->ToInclusive = true;
    }
    return request;
}

size_t CountRows(const TEvReadTableRangeResponse& response) {
    size_t rows = 0;
    for (const auto& batch : response.Batches) {
        rows += batch->Get()->GetRowsCount();
    }
    return rows;
}

TVector<std::pair<ui32, TString>> CollectRows(const TEvReadTableRangeResponse& response) {
    TVector<std::pair<ui32, TString>> rows;
    for (const auto& batch : response.Batches) {
        const TEvDataShard::TEvReadResult* result = batch->Get();
        for (size_t i = 0; i < result->GetRowsCount(); ++i) {
            const auto cells = result->GetCells(i);
            UNIT_ASSERT_VALUES_EQUAL(cells.size(), 2);
            rows.emplace_back(cells[0].AsValue<ui32>(), TString(cells[1].AsBuf()));
        }
    }
    return rows;
}

TEvReadTableRangeResponse::TPtr DoRead(TTestActorRuntime& runtime, TActorId reader, TActorId sender, THolder<TEvReadTableRangeRequest> request) {
    runtime.Send(new IEventHandle(reader, sender, request.Release()));
    auto response = runtime.GrabEdgeEventRethrow<TEvReadTableRangeResponse>(sender);
    UNIT_ASSERT_C(response->Get()->Success, response->Get()->Issues.ToOneLineString());
    return response;
}

TServer::TPtr StartServer(TPortManager& pm, TActorId& sender) {
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root");
    TServer::TPtr server = new TServer(serverSettings);
    auto& runtime = *server->GetRuntime();
    sender = runtime.AllocateEdgeActor();
    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);
    return server;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TTableRangeReaderTest) {

    Y_UNIT_TEST(ReadWholeTable) {
        TPortManager pm;
        TActorId sender;
        TServer::TPtr server = StartServer(pm, sender);
        auto& runtime = *server->GetRuntime();

        InitReaderTable(server, sender);

        TActorId reader = runtime.Register(CreateTableRangeReader());
        auto readerSender = runtime.AllocateEdgeActor();
        auto response = DoRead(runtime, reader, readerSender, MakeReadRequest(1, {}, {}));

        UNIT_ASSERT_VALUES_EQUAL(response->Get()->RequestId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(*response->Get()), 5u);

        auto rows = CollectRows(*response->Get());
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].first, 1u);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].second, "one");
        UNIT_ASSERT_VALUES_EQUAL(rows[2].first, 3u);
        UNIT_ASSERT_VALUES_EQUAL(rows[2].second, "three");
        UNIT_ASSERT_VALUES_EQUAL(rows[4].first, 5u);
        UNIT_ASSERT_VALUES_EQUAL(rows[4].second, "five");
    }

    Y_UNIT_TEST(ReadRangeWithPagination) {
        TPortManager pm;
        TActorId sender;
        TServer::TPtr server = StartServer(pm, sender);
        auto& runtime = *server->GetRuntime();

        InitReaderTable(server, sender);

        TActorId reader = runtime.Register(CreateTableRangeReader());
        auto readerSender = runtime.AllocateEdgeActor();
        gTableRangeReaderMaxRows = 2;
        Y_DEFER {
            gTableRangeReaderMaxRows = UINT64_MAX;
        };
        auto response = DoRead(runtime, reader, readerSender, MakeReadRequest(2, 2, 5));

        UNIT_ASSERT_VALUES_EQUAL(CountRows(*response->Get()), 4u);
        auto rows = CollectRows(*response->Get());
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4u);
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rows[i].first, static_cast<ui32>(i + 2));
        }
        UNIT_ASSERT_VALUES_EQUAL(rows[0].second, "two");
        UNIT_ASSERT_VALUES_EQUAL(rows[3].second, "five");
    }

    // The same reader serves several requests to one table. The navigation and
    // partitioning results are cached, so the second request must not need them
    // again and must still return correct rows.
    Y_UNIT_TEST(RepeatedRequestsReuseResolvedTable) {
        TPortManager pm;
        TActorId sender;
        TServer::TPtr server = StartServer(pm, sender);
        auto& runtime = *server->GetRuntime();

        InitReaderTable(server, sender);

        TActorId reader = runtime.Register(CreateTableRangeReader());
        auto readerSender = runtime.AllocateEdgeActor();

        auto first = DoRead(runtime, reader, readerSender, MakeReadRequest(10, 1, 3));
        UNIT_ASSERT_VALUES_EQUAL(CountRows(*first->Get()), 3u);
        auto firstRows = CollectRows(*first->Get());
        UNIT_ASSERT_VALUES_EQUAL(firstRows.front().first, 1u);
        UNIT_ASSERT_VALUES_EQUAL(firstRows.back().first, 3u);

        auto second = DoRead(runtime, reader, readerSender, MakeReadRequest(11, 4, 5));
        UNIT_ASSERT_VALUES_EQUAL(CountRows(*second->Get()), 2u);
        auto secondRows = CollectRows(*second->Get());
        UNIT_ASSERT_VALUES_EQUAL(secondRows.front().first, 4u);
        UNIT_ASSERT_VALUES_EQUAL(secondRows.back().first, 5u);
    }

    Y_UNIT_TEST(UnknownColumnFailsRequest) {
        TPortManager pm;
        TActorId sender;
        TServer::TPtr server = StartServer(pm, sender);
        auto& runtime = *server->GetRuntime();

        InitReaderTable(server, sender);

        TActorId reader = runtime.Register(CreateTableRangeReader());
        auto readerSender = runtime.AllocateEdgeActor();

        auto request = MakeReadRequest(20, {}, {});
        request->Columns = {"key", "no-such-column"};
        runtime.Send(new IEventHandle(reader, readerSender, request.Release()));

        auto response = runtime.GrabEdgeEventRethrow<TEvReadTableRangeResponse>(readerSender);
        UNIT_ASSERT(!response->Get()->Success);
        UNIT_ASSERT_STRING_CONTAINS(response->Get()->Issues.ToOneLineString(), "Unknown column");
    }

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr
