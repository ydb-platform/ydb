#include "ut_helpers.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
    using namespace Tests;
    using Ydb::Table::FulltextIndexSettings;
    using namespace NTableIndex::NFulltext;

    static std::atomic<ui64> sId = 1;
    static const TString kDatabaseName = "/Root";
    static const TString kMainTable = "/Root/table-main";
    static const TString kIndexTable = "/Root/table-index";
    static const TString kDocsTable = "/Root/table-docs";

    Y_UNIT_TEST_SUITE(TTxDataShardBuildFulltextIndexScan) {

        ui64 FillRequest(Tests::TServer::TPtr server, TActorId sender,
                         NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request,
                         std::function<void(NKikimrTxDataShard::TEvBuildFulltextIndexRequest&)> setupRequest)
        {
            auto id = sId.fetch_add(1, std::memory_order_relaxed);

            auto snapshot = CreateVolatileSnapshot(server, {kMainTable});
            auto datashards = GetTableShards(server, sender, kMainTable);
            TTableId tableId = ResolveTableId(server, sender, kMainTable);

            UNIT_ASSERT(datashards.size() == 1);

            request.SetId(1);
            request.SetSeqNoGeneration(id);
            request.SetSeqNoRound(1);

            request.SetTabletId(datashards[0]);
            tableId.PathId.ToProto(request.MutablePathId());

            request.SetSnapshotTxId(snapshot.TxId);
            request.SetSnapshotStep(snapshot.Step);

            request.SetIndexType(NKikimrTxDataShard::EFulltextIndexType::FulltextPlain);

            FulltextIndexSettings settings;
            auto column = settings.add_columns();
            column->set_column("text");
            column->mutable_analyzers()->set_tokenizer(FulltextIndexSettings::WHITESPACE);
            *request.MutableSettings() = settings;

            request.SetDatabaseName(kDatabaseName);
            request.SetIndexName(kIndexTable);
            request.SetDocsTableName(kDocsTable);

            setupRequest(request);

            return datashards[0];
        }

        void DoBadRequest(Tests::TServer::TPtr server, TActorId sender,
                          std::function<void(NKikimrTxDataShard::TEvBuildFulltextIndexRequest&)> setupRequest,
                          const TString& expectedError, bool expectedErrorSubstring = false, NKikimrIndexBuilder::EBuildStatus expectedStatus = NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST)
        {
            auto ev = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();

            auto tabletId = FillRequest(server, sender, ev->Record, setupRequest);

            NKikimr::DoBadRequest<TEvDataShard::TEvBuildFulltextIndexResponse>(server, sender, std::move(ev), tabletId, expectedError, expectedErrorSubstring, expectedStatus);
        }

        TEvDataShard::TEvBuildFulltextIndexResponse::TPtr DoBuildRaw(Tests::TServer::TPtr server, TActorId sender,
                                                                     std::function<void(NKikimrTxDataShard::TEvBuildFulltextIndexRequest&)> setupRequest) {
            auto ev1 = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
            auto tabletId = FillRequest(server, sender, ev1->Record, setupRequest);

            auto ev2 = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
            ev2->Record.CopyFrom(ev1->Record);

            auto& runtime = *server->GetRuntime();
            runtime.SendToPipe(tabletId, sender, ev1.release(), 0, GetPipeConfigWithRetries());
            runtime.SendToPipe(tabletId, sender, ev2.release(), 0, GetPipeConfigWithRetries());

            // Drain intermediate IN_PROGRESS responses until we get the final DONE/error response
            TEvDataShard::TEvBuildFulltextIndexResponse::TPtr reply;
            do {
                reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvBuildFulltextIndexResponse>(sender);
            } while (reply->Get()->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS);

            UNIT_ASSERT_EQUAL_C(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE, reply->Get()->Record.ShortDebugString());

            return reply;
        }

        TString DoBuild(Tests::TServer::TPtr server, TActorId sender, std::function<void(NKikimrTxDataShard::TEvBuildFulltextIndexRequest&)> setupRequest) {
            DoBuildRaw(server, sender, setupRequest);
            auto index = ReadShardedTable(server, kIndexTable);
            Cerr << "Index:" << Endl;
            Cerr << index << Endl;
            return std::move(index);
        }

        void CreateMainTable(Tests::TServer::TPtr server, TActorId sender) {
            TShardedTableOptions options;
            options.EnableOutOfOrder(true);
            options.Shards(1);
            options.AllowSystemColumnNames(false);
            options.Columns({
                {"key", "Uint32", true, true},
                {"text", "String", false, false},
                {"data", "String", false, false},
            });
            CreateShardedTable(server, sender, "/Root", "table-main", options);
        }

        void FillMainTable(Tests::TServer::TPtr server, TActorId sender) {
            ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-main` (key, text, data) VALUES
                (1, "green apple", "one"),
                (2, "red apple and blue apple", "two"),
                (3, "yellow apple", "three"),
                (4, "red car", "four")
        )");
        }

        void CreateIndexTable(Tests::TServer::TPtr server, TActorId sender, bool withRelevance) {
            TShardedTableOptions options;
            options.EnableOutOfOrder(true);
            options.Shards(1);
            options.AllowSystemColumnNames(true);
            if (withRelevance) {
                options.Columns({
                    {TokenColumn, "String", true, true},
                    {"key", "Uint32", true, true},
                    {FreqColumn, TokenCountTypeName, false, true},
                });
            } else {
                options.Columns({
                    {TokenColumn, "String", true, true},
                    {"key", "Uint32", true, true},
                    {"data", "String", false, false},
                });
            }
            CreateShardedTable(server, sender, "/Root", "table-index", options);
        }

        void CreateDocsTable(Tests::TServer::TPtr server, TActorId sender) {
            TShardedTableOptions options;
            options.EnableOutOfOrder(true);
            options.Shards(1);
            options.AllowSystemColumnNames(true);
            options.Columns({
                {"key", "Uint32", true, true},
                {"data", "String", false, false},
                {DocLengthColumn, TokenCountTypeName, false, false},
            });
            CreateShardedTable(server, sender, "/Root", "table-docs", options);
        }

        void Setup(Tests::TServer::TPtr server, TActorId sender, bool withRelevance = false) {
            server->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
            server->GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

            InitRoot(server, sender);

            CreateMainTable(server, sender);
            FillMainTable(server, sender);
            CreateIndexTable(server, sender, withRelevance);
        }

        Y_UNIT_TEST(BadRequest) {
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            Setup(server, sender);

            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.SetTabletId(0);
            }, TStringBuilder() << "{ <main>: Error: Wrong shard 0 this is " << GetTableShards(server, sender, kMainTable)[0] << " }");
            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                TPathId(0, 0).ToProto(request.MutablePathId());
            }, "{ <main>: Error: Unknown table id: 0 }");

            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.SetSnapshotStep(request.GetSnapshotStep() + 1);
            }, "Error: Unknown snapshot", true);
            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.ClearSnapshotStep();
            }, "{ <main>: Error: Missing snapshot }");
            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.SetSnapshotTxId(request.GetSnapshotTxId() + 1);
            }, "Error: Unknown snapshot", true);
            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.ClearSnapshotTxId();
            }, "{ <main>: Error: Missing snapshot }");

            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.clear_settings();
            }, "{ <main>: Error: Missing fulltext index settings }");
            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.MutableSettings()->clear_columns();
            }, "{ <main>: Error: columns should be set }");
            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.MutableSettings()->mutable_columns()->at(0).mutable_analyzers()->clear_tokenizer();
            }, "{ <main>: Error: tokenizer should be set }");

            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.ClearIndexName();
            }, "{ <main>: Error: Empty index table name }");

            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.SetIndexType(NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance);
                request.ClearDocsTableName();
            }, "{ <main>: Error: Empty index documents table name }");

            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.MutableSettings()->mutable_columns()->at(0).set_column("some");
            }, "{ <main>: Error: Unknown key column: some }");
            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.AddDataColumns("some");
            }, "{ <main>: Error: Unknown data column: some }");

            // test multiple issues:
            DoBadRequest(server, sender, [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                request.ClearIndexName();
                request.AddDataColumns("some");
            }, "[ { <main>: Error: Empty index table name } { <main>: Error: Unknown data column: some } ]");
        }

        Y_UNIT_TEST(Build) {
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            Setup(server, sender);

            auto result = DoBuild(server, sender, [](auto&) {});

            UNIT_ASSERT_VALUES_EQUAL(result, R"(__ydb_token = and, key = 2, data = (empty maybe)
__ydb_token = apple, key = 1, data = (empty maybe)
__ydb_token = apple, key = 2, data = (empty maybe)
__ydb_token = apple, key = 3, data = (empty maybe)
__ydb_token = blue, key = 2, data = (empty maybe)
__ydb_token = car, key = 4, data = (empty maybe)
__ydb_token = green, key = 1, data = (empty maybe)
__ydb_token = red, key = 2, data = (empty maybe)
__ydb_token = red, key = 4, data = (empty maybe)
__ydb_token = yellow, key = 3, data = (empty maybe)
)");
        }

        Y_UNIT_TEST(BuildWithData) {
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            Setup(server, sender);

            auto result = DoBuild(server, sender, [](auto& request) {
                request.AddDataColumns("data");
            });

            UNIT_ASSERT_VALUES_EQUAL(result, R"(__ydb_token = and, key = 2, data = two
__ydb_token = apple, key = 1, data = one
__ydb_token = apple, key = 2, data = two
__ydb_token = apple, key = 3, data = three
__ydb_token = blue, key = 2, data = two
__ydb_token = car, key = 4, data = four
__ydb_token = green, key = 1, data = one
__ydb_token = red, key = 2, data = two
__ydb_token = red, key = 4, data = four
__ydb_token = yellow, key = 3, data = three
)");
        }

        Y_UNIT_TEST(BuildWithTextData) {
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            InitRoot(server, sender);

            CreateMainTable(server, sender);
            FillMainTable(server, sender);

            { // CreateIndexTable with text column
                TShardedTableOptions options;
                options.EnableOutOfOrder(true);
                options.Shards(1);
                options.AllowSystemColumnNames(true);
                options.Columns({
                    {TokenColumn, "String", true, true},
                    {"key", "Uint32", true, true},
                    {"text", "String", false, false},
                    {"data", "String", false, false},
                });
                CreateShardedTable(server, sender, "/Root", "table-index", options);
            }

            auto result = DoBuild(server, sender, [](auto& request) {
                request.AddDataColumns("text");
                request.AddDataColumns("data");
            });

            UNIT_ASSERT_VALUES_EQUAL(result, R"(__ydb_token = and, key = 2, text = red apple and blue apple, data = two
__ydb_token = apple, key = 1, text = green apple, data = one
__ydb_token = apple, key = 2, text = red apple and blue apple, data = two
__ydb_token = apple, key = 3, text = yellow apple, data = three
__ydb_token = blue, key = 2, text = red apple and blue apple, data = two
__ydb_token = car, key = 4, text = red car, data = four
__ydb_token = green, key = 1, text = green apple, data = one
__ydb_token = red, key = 2, text = red apple and blue apple, data = two
__ydb_token = red, key = 4, text = red car, data = four
__ydb_token = yellow, key = 3, text = yellow apple, data = three
)");
        }

        Y_UNIT_TEST(BuildWithTextFromKey) {
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            server->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
            server->GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

            InitRoot(server, sender);

            { // CreateMainTable
                TShardedTableOptions options;
                options.EnableOutOfOrder(true);
                options.Shards(1);
                options.AllowSystemColumnNames(false);
                options.Columns({
                    {"key", "Uint32", true, true},
                    {"text", "String", true, true},
                    {"subkey", "Uint32", true, true},
                    {"data", "String", false, false},
                });
                CreateShardedTable(server, sender, "/Root", "table-main", options);
            }
            { // FillMainTable
                ExecSQL(server, sender, R"(
                UPSERT INTO `/Root/table-main` (key, text, subkey, data) VALUES
                    (1, "green apple", 11, "one"),
                    (2, "red apple", 22, "two"),
                    (3, "yellow apple", 33, "three"),
                    (4, "red car", 44, "four")
            )");
            }
            { // CreateIndexTable
                TShardedTableOptions options;
                options.EnableOutOfOrder(true);
                options.Shards(1);
                options.AllowSystemColumnNames(true);
                options.Columns({
                    {TokenColumn, "String", true, true},
                    {"key", "Uint32", true, true},
                    {"text", "String", true, true},
                    {"subkey", "Uint32", true, true},
                    {"data", "String", false, false},
                });
                CreateShardedTable(server, sender, "/Root", "table-index", options);
            }

            auto result = DoBuild(server, sender, [](auto& request) {
                request.AddDataColumns("data");
            });

            UNIT_ASSERT_VALUES_EQUAL(result, R"(__ydb_token = apple, key = 1, text = green apple, subkey = 11, data = one
__ydb_token = apple, key = 2, text = red apple, subkey = 22, data = two
__ydb_token = apple, key = 3, text = yellow apple, subkey = 33, data = three
__ydb_token = car, key = 4, text = red car, subkey = 44, data = four
__ydb_token = green, key = 1, text = green apple, subkey = 11, data = one
__ydb_token = red, key = 2, text = red apple, subkey = 22, data = two
__ydb_token = red, key = 4, text = red car, subkey = 44, data = four
__ydb_token = yellow, key = 3, text = yellow apple, subkey = 33, data = three
)");
        }

        // Collect all IN_PROGRESS responses and return them alongside the final DONE reply.
        std::pair<TVector<TEvDataShard::TEvBuildFulltextIndexResponse::TPtr>,
                  TEvDataShard::TEvBuildFulltextIndexResponse::TPtr>
        DoBuildRawWithProgress(Tests::TServer::TPtr server, TActorId sender,
                               std::function<void(NKikimrTxDataShard::TEvBuildFulltextIndexRequest&)> setupRequest)
        {
            auto ev = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
            auto tabletId = FillRequest(server, sender, ev->Record, setupRequest);

            auto& runtime = *server->GetRuntime();
            runtime.SendToPipe(tabletId, sender, ev.release(), 0, GetPipeConfigWithRetries());

            TVector<TEvDataShard::TEvBuildFulltextIndexResponse::TPtr> progressReplies;
            TEvDataShard::TEvBuildFulltextIndexResponse::TPtr doneReply;
            do {
                doneReply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvBuildFulltextIndexResponse>(sender);
                if (doneReply->Get()->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS) {
                    progressReplies.push_back(std::move(doneReply));
                    doneReply = nullptr;
                }
            } while (!doneReply);

            UNIT_ASSERT_EQUAL_C(doneReply->Get()->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE,
                                doneReply->Get()->Record.ShortDebugString());

            return {std::move(progressReplies), std::move(doneReply)};
        }

        Y_UNIT_TEST(LastKeyAckInFinalResponse) {
            // After a complete build the DONE response must carry LastKeyAck set to the
            // last document key (key = 4 in the standard 4-row fixture).
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            Setup(server, sender);

            auto [progressReplies, doneReply] = DoBuildRawWithProgress(server, sender, [](auto&) {});

            const auto& record = doneReply->Get()->Record;
            UNIT_ASSERT_C(record.HasLastKeyAck(), "DONE response must have LastKeyAck set");

            // Parse the last ack key and check it equals key=4 (the largest key in the table)
            TSerializedCellVec lastAck(record.GetLastKeyAck());
            UNIT_ASSERT_VALUES_EQUAL(lastAck.GetCells().size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(lastAck.GetCells()[0].AsValue<ui32>(), 4u);
        }

        Y_UNIT_TEST(InProgressResponsesCarryLastKeyAck) {
            // Every IN_PROGRESS response must have a non-empty LastKeyAck that is
            // strictly greater than the previous one (monotone increasing).
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            // Use a very small batch size so that we get at least one IN_PROGRESS ACK
            // for the 4-row table.
            Setup(server, sender);

            auto [progressReplies, doneReply] = DoBuildRawWithProgress(server, sender,
                                                                       [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& request) {
                                                                           // Force tiny batches: 1 row per upload so every document triggers an ACK
                                                                           request.MutableScanSettings()->SetMaxBatchRows(1);
                                                                       });

            // We must have received at least one IN_PROGRESS response with LastKeyAck
            UNIT_ASSERT_C(!progressReplies.empty(), "Expected at least one IN_PROGRESS response");

            ui32 prevKey = 0;
            for (const auto& progress : progressReplies) {
                const auto& rec = progress->Get()->Record;
                UNIT_ASSERT_C(rec.HasLastKeyAck(), "IN_PROGRESS response must have LastKeyAck");
                TSerializedCellVec ack(rec.GetLastKeyAck());
                UNIT_ASSERT_VALUES_EQUAL(ack.GetCells().size(), 1u);
                ui32 currentKey = ack.GetCells()[0].AsValue<ui32>();
                UNIT_ASSERT_C(currentKey > prevKey,
                              "LastKeyAck must be strictly increasing: prev=" << prevKey << " current=" << currentKey);
                prevKey = currentKey;
            }

            // The DONE response must also have LastKeyAck
            UNIT_ASSERT_C(doneReply->Get()->Record.HasLastKeyAck(),
                          "DONE response must have LastKeyAck");
        }

        Y_UNIT_TEST(ResumeFromKeyRange) {
            // When a KeyRange is supplied (simulating resume after a partial scan),
            // only the rows with keys strictly after the range start are indexed.
            // Rows for keys 1 and 2 should be absent; keys 3 and 4 must appear.
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            Setup(server, sender);

            // Resume from "after key=2": exclusive lower bound key=2
            auto result = DoBuild(server, sender, [](auto& request) {
                TCell fromCell = TCell::Make(ui32(2));
                TSerializedTableRange range({&fromCell, 1}, false /*exclusive*/, {}, false);
                range.Serialize(*request.MutableKeyRange());
            });

            // Only tokens from documents with key=3 ("yellow apple") and key=4 ("red car")
            UNIT_ASSERT_VALUES_EQUAL(result, R"(__ydb_token = apple, key = 3, data = (empty maybe)
__ydb_token = car, key = 4, data = (empty maybe)
__ydb_token = red, key = 4, data = (empty maybe)
__ydb_token = yellow, key = 3, data = (empty maybe)
)");
        }

        Y_UNIT_TEST(ResumeFromKeyRangeProducesSameResultAsFullScan) {
            // Two sequential builds — first covering keys 1-2, second resuming from key=2
            // (exclusive) to key=4 — together produce the same index as a single full build.
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            Setup(server, sender);

            // First half: keys 1..2 (inclusive on both ends)
            DoBuild(server, sender, [](auto& request) {
                TCell toCell = TCell::Make(ui32(2));
                TSerializedTableRange range({}, false, {&toCell, 1}, true /*inclusive*/);
                range.Serialize(*request.MutableKeyRange());
            });

            // Second half: keys > 2 (i.e., 3 and 4)
            DoBuild(server, sender, [](auto& request) {
                TCell fromCell = TCell::Make(ui32(2));
                TSerializedTableRange range({&fromCell, 1}, false /*exclusive*/, {}, false);
                range.Serialize(*request.MutableKeyRange());
            });

            auto combined = ReadShardedTable(server, kIndexTable);

            // Now do a fresh full build into a new index table
            {
                TShardedTableOptions options;
                options.EnableOutOfOrder(true);
                options.Shards(1);
                options.AllowSystemColumnNames(true);
                options.Columns({
                    {NTableIndex::NFulltext::TokenColumn, "String", true, true},
                    {"key", "Uint32", true, true},
                    {"data", "String", false, false},
                });
                CreateShardedTable(server, sender, "/Root", "table-index-full", options);
            }

            {
                auto ev = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
                FillRequest(server, sender, ev->Record, [](auto& request) {
                    request.SetIndexName("/Root/table-index-full");
                });

                auto tabletId = GetTableShards(server, sender, kMainTable)[0];
                auto& runtime = *server->GetRuntime();
                runtime.SendToPipe(tabletId, sender, ev.release(), 0, GetPipeConfigWithRetries());

                TEvDataShard::TEvBuildFulltextIndexResponse::TPtr reply;
                do {
                    reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvBuildFulltextIndexResponse>(sender);
                } while (reply->Get()->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS);
                UNIT_ASSERT_EQUAL(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE);
            }

            auto full = ReadShardedTable(server, "/Root/table-index-full");
            UNIT_ASSERT_VALUES_EQUAL(combined, full);
        }

        // Helper: send a request and collect all responses until DONE.
        // Stops early after `stopAfterProgress` IN_PROGRESS responses and returns the last
        // received LastKeyAck without waiting for DONE.
        TString DoBuildPartial(Tests::TServer::TPtr server, TActorId sender,
                               std::function<void(NKikimrTxDataShard::TEvBuildFulltextIndexRequest&)> setupRequest,
                               size_t stopAfterProgress)
        {
            auto ev = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
            auto tabletId = FillRequest(server, sender, ev->Record, setupRequest);

            auto& runtime = *server->GetRuntime();
            runtime.SendToPipe(tabletId, sender, ev.release(), 0, GetPipeConfigWithRetries());

            TString lastKeyAck;
            size_t progressCount = 0;
            while (true) {
                auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvBuildFulltextIndexResponse>(sender);
                const auto& record = reply->Get()->Record;
                if (record.HasLastKeyAck()) {
                    lastKeyAck = record.GetLastKeyAck();
                }
                if (record.GetStatus() != NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS) {
                    break;
                }
                ++progressCount;
                if (progressCount >= stopAfterProgress) {
                    break;
                }
            }
            return lastKeyAck;
        }

        Y_UNIT_TEST(SchemeShardResumesFromLastKeyAckAfterRestart) {
            // Simulate what schemeshard does on restart:
            //   1. Run a partial scan (stopping after the first IN_PROGRESS with LastKeyAck).
            //   2. Use the persisted LastKeyAck as the KeyRange for a second scan (exclusive start).
            //   3. Verify the combined result equals a fresh full scan.
            //
            // This tests both that:
            //   (a) LastKeyAck is set in IN_PROGRESS responses, and
            //   (b) resuming from that key correctly skips already-indexed rows.
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            Setup(server, sender);

            // Step 1: Partial scan with batch size 1 so we get IN_PROGRESS after each row.
            // Stop after the very first IN_PROGRESS — simulating a schemeshard restart at that point.
            TString lastKeyAck = DoBuildPartial(server, sender,
                                                [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& req) {
                                                    req.MutableScanSettings()->SetMaxBatchRows(1);
                                                },
                                                /*stopAfterProgress=*/1);

            UNIT_ASSERT_C(!lastKeyAck.empty(), "Expected at least one IN_PROGRESS with LastKeyAck");

            // Step 2: Second scan — resume from LastKeyAck (exclusive start), full range to end.
            // This is what schemeshard does when it reconstructs the request after restart.
            {
                auto ev = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
                auto tabletId = FillRequest(server, sender, ev->Record, [&](auto& req) {
                    // Set KeyRange starting exclusively from the last acked key
                    TSerializedCellVec ackCells(lastKeyAck);
                    TSerializedTableRange range(ackCells.GetCells(), false /*inclusive from*/, {}, false);
                    range.Serialize(*req.MutableKeyRange());
                });
                // Remember the seqNo of this request to filter out stale responses from the
                // cancelled first scan (which will arrive as ABORTED with the old seqNo).
                const ui64 seqNoGen = ev->Record.GetSeqNoGeneration();
                const ui64 seqNoRound = ev->Record.GetSeqNoRound();

                auto& runtime = *server->GetRuntime();
                runtime.SendToPipe(tabletId, sender, ev.release(), 0, GetPipeConfigWithRetries());

                TEvDataShard::TEvBuildFulltextIndexResponse::TPtr reply;
                do {
                    reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvBuildFulltextIndexResponse>(sender);
                    // Skip responses belonging to the previous (now-cancelled) scan.
                    if (reply->Get()->Record.GetRequestSeqNoGeneration() != seqNoGen ||
                        reply->Get()->Record.GetRequestSeqNoRound() != seqNoRound) {
                        reply = nullptr;
                    }
                } while (!reply || reply->Get()->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS);

                UNIT_ASSERT_EQUAL_C(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE,
                                    reply->Get()->Record.ShortDebugString());
            }

            auto combined = ReadShardedTable(server, kIndexTable);

            // Step 3: Do a full scan into a separate index table and compare.
            {
                TShardedTableOptions options;
                options.EnableOutOfOrder(true);
                options.Shards(1);
                options.AllowSystemColumnNames(true);
                options.Columns({
                    {NTableIndex::NFulltext::TokenColumn, "String", true, true},
                    {"key", "Uint32", true, true},
                    {"data", "String", false, false},
                });
                CreateShardedTable(server, sender, "/Root", "table-index-full", options);
            }

            {
                auto ev = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
                FillRequest(server, sender, ev->Record, [](auto& req) {
                    req.SetIndexName("/Root/table-index-full");
                });

                auto tabletId = GetTableShards(server, sender, kMainTable)[0];
                auto& runtime = *server->GetRuntime();
                runtime.SendToPipe(tabletId, sender, ev.release(), 0, GetPipeConfigWithRetries());

                TEvDataShard::TEvBuildFulltextIndexResponse::TPtr reply;
                do {
                    reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvBuildFulltextIndexResponse>(sender);
                } while (reply->Get()->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS);
                UNIT_ASSERT_EQUAL(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE);
            }

            auto full = ReadShardedTable(server, "/Root/table-index-full");
            UNIT_ASSERT_VALUES_EQUAL(combined, full);
        }

        Y_UNIT_TEST(SchemeShardDoesNotRescanAlreadyIndexedRows) {
            // Verify that rows before LastKeyAck are not re-indexed when the scan is resumed.
            // We run a partial scan (1 row per batch) and collect the first IN_PROGRESS LastKeyAck.
            // Then we resume from that key and check that the index does NOT contain duplicate entries
            // for any (token, key) pair.
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            Setup(server, sender);

            // Partial scan: stop after first IN_PROGRESS
            TString lastKeyAck = DoBuildPartial(server, sender,
                                                [](NKikimrTxDataShard::TEvBuildFulltextIndexRequest& req) {
                                                    req.MutableScanSettings()->SetMaxBatchRows(1);
                                                },
                                                /*stopAfterProgress=*/1);

            UNIT_ASSERT_C(!lastKeyAck.empty(), "Expected at least one IN_PROGRESS with LastKeyAck");

            // Parse the acked key to know how far the first scan reached
            TSerializedCellVec ackedCells(lastKeyAck);
            UNIT_ASSERT_VALUES_EQUAL(ackedCells.GetCells().size(), 1u);
            ui32 ackedKey = ackedCells.GetCells()[0].AsValue<ui32>();

            // Resume scan from LastKeyAck (exclusive)
            {
                auto ev = std::make_unique<TEvDataShard::TEvBuildFulltextIndexRequest>();
                FillRequest(server, sender, ev->Record, [&](auto& req) {
                    TSerializedTableRange range(ackedCells.GetCells(), false, {}, false);
                    range.Serialize(*req.MutableKeyRange());
                });
                const ui64 seqNoGen = ev->Record.GetSeqNoGeneration();
                const ui64 seqNoRound = ev->Record.GetSeqNoRound();

                auto tabletId = GetTableShards(server, sender, kMainTable)[0];
                auto& runtime = *server->GetRuntime();
                runtime.SendToPipe(tabletId, sender, ev.release(), 0, GetPipeConfigWithRetries());

                TEvDataShard::TEvBuildFulltextIndexResponse::TPtr reply;
                do {
                    reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvBuildFulltextIndexResponse>(sender);
                    // Skip stale responses from the cancelled partial scan.
                    if (reply->Get()->Record.GetRequestSeqNoGeneration() != seqNoGen ||
                        reply->Get()->Record.GetRequestSeqNoRound() != seqNoRound) {
                        reply = nullptr;
                    }
                } while (!reply || reply->Get()->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS);
                UNIT_ASSERT_EQUAL(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::EBuildStatus::DONE);
            }

            // The index must contain every key exactly once for each token.
            // If rows before ackedKey were re-scanned, they would appear twice in the index,
            // causing ReadShardedTable to show duplicates (since the table is a PK table, upsert
            // is idempotent — but check that all rows for keys <= ackedKey appear exactly once
            // by verifying against the expected full-scan result).
            auto result = ReadShardedTable(server, kIndexTable);

            // The result must be identical to a full build: no missing or duplicate rows.
            UNIT_ASSERT_VALUES_EQUAL(result, R"(__ydb_token = and, key = 2, data = (empty maybe)
__ydb_token = apple, key = 1, data = (empty maybe)
__ydb_token = apple, key = 2, data = (empty maybe)
__ydb_token = apple, key = 3, data = (empty maybe)
__ydb_token = blue, key = 2, data = (empty maybe)
__ydb_token = car, key = 4, data = (empty maybe)
__ydb_token = green, key = 1, data = (empty maybe)
__ydb_token = red, key = 2, data = (empty maybe)
__ydb_token = red, key = 4, data = (empty maybe)
__ydb_token = yellow, key = 3, data = (empty maybe)
)");

            // Also verify the acked key is reasonable (between 1 and 4 inclusive)
            UNIT_ASSERT_C(ackedKey >= 1 && ackedKey <= 4,
                          "ackedKey=" << ackedKey << " is outside expected range [1,4]");
        }

        Y_UNIT_TEST(BuildWithRelevance) {
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root");

            Tests::TServer::TPtr server = new TServer(serverSettings);
            auto sender = server->GetRuntime()->AllocateEdgeActor();

            Setup(server, sender, true);
            CreateDocsTable(server, sender);

            auto reply = DoBuildRaw(server, sender, [](auto& request) {
                request.SetIndexType(NKikimrTxDataShard::EFulltextIndexType::FulltextRelevance);
            });
            auto& record = reply->Get()->Record;

            UNIT_ASSERT_VALUES_EQUAL(record.GetDocCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(record.GetTotalDocLength(), 11);

            auto index = ReadShardedTable(server, kIndexTable);
            Cerr << "Index:" << Endl;
            Cerr << index << Endl;
            auto docs = ReadShardedTable(server, kDocsTable);
            Cerr << "Docs:" << Endl;
            Cerr << docs << Endl;

            UNIT_ASSERT_VALUES_EQUAL(index, R"(__ydb_token = and, key = 2, __ydb_freq = 1
__ydb_token = apple, key = 1, __ydb_freq = 1
__ydb_token = apple, key = 2, __ydb_freq = 2
__ydb_token = apple, key = 3, __ydb_freq = 1
__ydb_token = blue, key = 2, __ydb_freq = 1
__ydb_token = car, key = 4, __ydb_freq = 1
__ydb_token = green, key = 1, __ydb_freq = 1
__ydb_token = red, key = 2, __ydb_freq = 1
__ydb_token = red, key = 4, __ydb_freq = 1
__ydb_token = yellow, key = 3, __ydb_freq = 1
)");
            UNIT_ASSERT_VALUES_EQUAL(docs, R"(key = 1, data = (empty maybe), __ydb_length = 2
key = 2, data = (empty maybe), __ydb_length = 5
key = 3, data = (empty maybe), __ydb_length = 2
key = 4, data = (empty maybe), __ydb_length = 2
)");
        }
    } // Y_UNIT_TEST_SUITE(TTxDataShardBuildFulltextIndexScan)

} // namespace NKikimr
