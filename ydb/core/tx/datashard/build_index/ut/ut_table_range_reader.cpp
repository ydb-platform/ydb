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

    TVector<TCell> cells;
    if (from) {
        cells.emplace_back(TCell::Make<ui32>(*from));
    } else {
        // -inf = {NULL}, +inf = {}
        cells.resize(1);
    }
    request->FromKey = TSerializedCellVec(cells);
    request->FromInclusive = true;
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

TActorId StartReader(TTestActorRuntime& runtime) {
    TActorId reader = runtime.Register(CreateTableRangeReader());
    // In a single-threaded runtime Send() delivers an event directly into
    // IActor::Receive, so the first request would overtake the Bootstrap event
    // that is still queued in the mailbox. Let the runtime dispatch it first.
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
    return reader;
}

TServer::TPtr StartServer(TPortManager& pm, TActorId& sender) {
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings
        .SetDomainName("Root")
        .SetUseRealThreads(false);
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

        TActorId reader = StartReader(runtime);
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

        TActorId reader = StartReader(runtime);
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

        TActorId reader = StartReader(runtime);
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

    // A shard that has split answers NOT_FOUND: retrying the same tablet can
    // never succeed, the range now belongs to other shards. The reader must
    // refresh the cached scheme and re-read the range from the new shards.
    Y_UNIT_TEST(RetriesSplitShard) {
        TPortManager pm;
        TActorId sender;
        TServer::TPtr server = StartServer(pm, sender);
        auto& runtime = *server->GetRuntime();

        InitReaderTable(server, sender);

        TActorId reader = StartReader(runtime);
        auto readerSender = runtime.AllocateEdgeActor();

        // Resolve the table before the split, so the reader caches the
        // partitioning that is about to become outdated.
        auto before = DoRead(runtime, reader, readerSender, MakeReadRequest(30, {}, {}));
        UNIT_ASSERT_VALUES_EQUAL(CountRows(*before->Get()), 5u);

        auto shards = GetTableShards(server, sender, kReaderTable);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(&runtime, -1);

        auto splitSender = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, splitSender, kReaderTable, shards.at(0), 3);
        WaitTxNotification(server, splitSender, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetTableShards(server, sender, kReaderTable).size(), 2u);

        runtime.Send(new IEventHandle(reader, readerSender, MakeReadRequest(31, {}, {}).Release()));
        auto response = runtime.GrabEdgeEventRethrow<TEvReadTableRangeResponse>(readerSender, TDuration::Seconds(30));
        UNIT_ASSERT_C(response->Get()->Success, response->Get()->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->RequestId, 31u);

        auto rows = CollectRows(*response->Get());
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 5u);
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rows[i].first, static_cast<ui32>(i + 1));
        }
    }

    // The cached navigation result carries the table schema version, so after
    // an alter the shard rejects reads with SCHEME_ERROR. The reader must
    // re-navigate the table and retry with the new schema version.
    Y_UNIT_TEST(RetriesAlteredTable) {
        TPortManager pm;
        TActorId sender;
        TServer::TPtr server = StartServer(pm, sender);
        auto& runtime = *server->GetRuntime();

        InitReaderTable(server, sender);

        TActorId reader = StartReader(runtime);
        auto readerSender = runtime.AllocateEdgeActor();

        auto before = DoRead(runtime, reader, readerSender, MakeReadRequest(40, {}, {}));
        UNIT_ASSERT_VALUES_EQUAL(CountRows(*before->Get()), 5u);

        ExecSQL(server, sender, "ALTER TABLE `/Root/table-reader` ADD COLUMN extra Uint32;", false);

        runtime.Send(new IEventHandle(reader, readerSender, MakeReadRequest(41, {}, {}).Release()));
        auto response = runtime.GrabEdgeEventRethrow<TEvReadTableRangeResponse>(readerSender, TDuration::Seconds(30));
        UNIT_ASSERT_C(response->Get()->Success, response->Get()->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(CountRows(*response->Get()), 5u);
    }

    // A shard that has moved to another node answers UNAVAILABLE for a moment.
    // Such a read must be retried on the same shard, continuing from the last
    // processed key instead of restarting the whole request.
    Y_UNIT_TEST(RetriesMovedShard) {
        TPortManager pm;
        TActorId sender;
        TServer::TPtr server = StartServer(pm, sender);
        auto& runtime = *server->GetRuntime();

        InitReaderTable(server, sender);

        TActorId reader = StartReader(runtime);
        auto readerSender = runtime.AllocateEdgeActor();

        auto before = DoRead(runtime, reader, readerSender, MakeReadRequest(50, {}, {}));
        UNIT_ASSERT_VALUES_EQUAL(CountRows(*before->Get()), 5u);

        // Drop the next read without delivering it to the shard, as if the
        // shard had just moved away.
        ui64 rejectedReadId = 0;
        TActorId rejectedTablet;
        size_t schemeRequests = 0;
        runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
            const ui32 type = event->GetTypeRewrite();
            if (type == TEvTxProxySchemeCache::TEvNavigateKeySet::EventType ||
                type == TEvTxProxySchemeCache::TEvResolveKeySet::EventType) {
                ++schemeRequests;
            }
            if (rejectedReadId || type != TEvDataShard::TEvRead::EventType) {
                return false;
            }
            rejectedReadId = event->Get<TEvDataShard::TEvRead>()->Record.GetReadId();
            rejectedTablet = event->Recipient;
            return true;
        });

        runtime.Send(new IEventHandle(reader, readerSender, MakeReadRequest(51, {}, {}).Release()));
        runtime.WaitFor("read to be intercepted", [&] { return rejectedReadId != 0; }, TDuration::Seconds(10));

        auto error = MakeHolder<TEvDataShard::TEvReadResult>();
        error->Record.SetReadId(rejectedReadId);
        error->Record.MutableStatus()->SetCode(Ydb::StatusIds::UNAVAILABLE);
        runtime.Send(new IEventHandle(reader, rejectedTablet, error.Release()));

        auto response = runtime.GrabEdgeEventRethrow<TEvReadTableRangeResponse>(readerSender, TDuration::Seconds(30));
        UNIT_ASSERT_C(response->Get()->Success, response->Get()->Issues.ToOneLineString());
        // The read was retried on the same shard, the table was not re-resolved.
        UNIT_ASSERT_VALUES_EQUAL(schemeRequests, 0u);

        auto rows = CollectRows(*response->Get());
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 5u);
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rows[i].first, static_cast<ui32>(i + 1));
        }
    }

    Y_UNIT_TEST(UnknownColumnFailsRequest) {
        TPortManager pm;
        TActorId sender;
        TServer::TPtr server = StartServer(pm, sender);
        auto& runtime = *server->GetRuntime();

        InitReaderTable(server, sender);

        TActorId reader = StartReader(runtime);
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
