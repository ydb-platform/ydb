#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/read_table.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>

#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(ExternalBlobsMultipleChannels) {

    struct TEvPutChannelCounter {
        std::unordered_map<ui8, ui32> PutsByChannel;
    };

    inline auto KqpExec(TTestActorRuntime& runtime, const TString& query) {
        return KqpSimpleSend(runtime, query, false, "/Root");
    }

    inline auto KqpExecSync(TTestActorRuntime& runtime, const TString& query) {
        return AwaitResponse(runtime, KqpSimpleSend(runtime, query, false, "/Root"));
    }

    std::shared_ptr<TEvPutChannelCounter> SetupPutCounter(TTestActorRuntime& runtime, ui32 blobSize = 2_KB) {
        auto putsCounter = std::make_shared<TEvPutChannelCounter>();

        auto captureEvents = [blobSize, putsCounter](TAutoPtr<IEventHandle> &event) -> auto {
            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvPut: {
                    auto* msg = event->Get<TEvBlobStorage::TEvPut>();
                    auto& logoblobId = msg->Id;
                    if ((logoblobId.BlobSize() - 8) == blobSize) {
                        putsCounter->PutsByChannel[msg->Id.Channel()]++;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        return putsCounter;
    }

    struct TNode {
        TPortManager Pm;
        TServerSettings ServerSettings;
        TServer::TPtr Server;
        ui64 Shard;
        TTableId TableId;
        TActorId Sender;
        TTestActorRuntime* Runtime;

        TNode(int externalBlobColumns, ui8 externalChannelsCount = 2) : ServerSettings(Pm.GetPort(2134)) {
            ServerSettings.SetDomainName("Root")
                .SetUseRealThreads(false)
                .AddStoragePool("ssd")
                .AddStoragePool("hdd")
                .AddStoragePool("ext", "ext", 10)
                .SetEnableUuidAsPrimaryKey(true);

            Server = new TServer(ServerSettings);
            
            Runtime = Server->GetRuntime();
            Runtime->SetLogPriority(NKikimrServices::KQP_SLOW_LOG, NLog::PRI_ERROR);

            Sender = Runtime->AllocateEdgeActor();
        
            InitRoot(Server, Sender);
            
            TShardedTableOptions::TFamily fam = {
                .Name = "default",
                .LogPoolKind = "ssd",
                .SysLogPoolKind = "ssd",
                .DataPoolKind = "ssd",
                .ExternalPoolKind = "ext",
                .DataThreshold = 100u,
                .ExternalThreshold = 1_KB,
                .ExternalChannelsCount = externalChannelsCount
            };
            
            TVector<TShardedTableOptions::TColumn> columns = {
                    {"blob_id", "Int32", true, false}
            };

            for (int i = 0; i < externalBlobColumns; i++) {
                columns.push_back({"data" + ToString(i), "String", false, false});
            }

            auto opts = TShardedTableOptions()
                .Columns(columns)
                .Families({fam, {
                    .Name = "non_default",
                    .DataPoolKind = "ssd",
                }});

            CreateShardedTable(Server, Sender, "/Root", "table-1", opts);

            Shard = GetTableShards(Server, Sender, "/Root/table-1").at(0);
            TableId = ResolveTableId(Server, Sender, "/Root/table-1");
        }
    };
    
    void ValidateReadResult(TTestActorRuntime& runtime,
            NThreading::TFuture<Ydb::Table::ExecuteDataQueryResponse> readFuture, ui64 blobSize = 2_KB, ui8 externalBlobColumns = 1) {
        Ydb::Table::ExecuteDataQueryResponse res = AwaitResponse(runtime, std::move(readFuture));
        auto& operation = res.Getoperation();
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        operation.result().UnpackTo(&result);
        UNIT_ASSERT_EQUAL(result.result_sets().size(), 1);
        auto& resultSet = result.result_sets()[0];
        UNIT_ASSERT_EQUAL(size_t(resultSet.rows_size()), 100);

        for (int i = 0; i < resultSet.rows_size(); i++) {
            auto& row = resultSet.get_idx_rows(i);

            UNIT_ASSERT_EQUAL(row.items_size(), 1 + externalBlobColumns);

            auto& blobIdValue = row.get_idx_items(0);

            UNIT_ASSERT(blobIdValue.has_int32_value());
            UNIT_ASSERT_EQUAL(blobIdValue.Getint32_value(), i);

            for (int j = 0; j < externalBlobColumns; j++) {
                auto& dataValue = row.get_idx_items(j + 1);
                UNIT_ASSERT(dataValue.has_bytes_value());
                UNIT_ASSERT_EQUAL(dataValue.bytes_value().size(), blobSize);
            }
        }
    }

    std::unique_ptr<TEvSchemeShard::TEvModifySchemeTransaction> CreateRequest(ui64 schemeShardId, ui64 txId, const TString& parentPath, const TString& scheme) {
        auto ev = new TEvSchemeShard::TEvModifySchemeTransaction(txId, schemeShardId);
        
        NKikimrSchemeOp::TModifyScheme* modifyScheme = ev->Record.AddTransaction();

        modifyScheme->SetOperationType(NKikimrSchemeOp ::EOperationType ::ESchemeOpAlterTable);
        modifyScheme->SetWorkingDir(parentPath);

        const bool ok = google::protobuf::TextFormat::ParseFromString(scheme, modifyScheme->MutableAlterTable());
        UNIT_ASSERT_C(ok, "protobuf parsing failed");

        return std::unique_ptr<TEvSchemeShard::TEvModifySchemeTransaction>(ev);
    }

    Y_UNIT_TEST(Simple) {
        TNode node(1);

        auto server = node.Server;
        auto& runtime = *node.Runtime;
        auto& sender = node.Sender;

        TString largeValue(2_KB, 'L');

        auto putCounter = SetupPutCounter(runtime);

        for (int i = 0; i < 100; i++) {
            TString chunkNum = ToString(i);
            TString query = "UPSERT INTO `/Root/table-1` (blob_id, data0) VALUES(" + chunkNum + ", \"" + largeValue + "\");";
            
            KqpExecSync(runtime, query);
        }

        auto shard1 = node.Shard;
        auto tableId1 = node.TableId;
        
        CompactTable(runtime, shard1, tableId1, false);

        RebootTablet(runtime, shard1, sender);

        auto readFuture = KqpExec(runtime, "SELECT blob_id, data0 FROM `/Root/table-1`;");

        ValidateReadResult(runtime, std::move(readFuture));

        UNIT_ASSERT_VALUES_EQUAL(putCounter->PutsByChannel.size(), 2);
    }

    Y_UNIT_TEST(WithCompaction) {
        TNode node(1);

        auto server = node.Server;
        auto& runtime = *node.Runtime;
        auto& sender = node.Sender;
        auto shard1 = node.Shard;
        auto tableId1 = node.TableId;

        TString largeValue(512_B, 'L');

        auto putCounter = SetupPutCounter(runtime, 512_B);

        for (int i = 0; i < 100; i++) {
            TString chunkNum = ToString(i);
            TString query = "UPSERT INTO `/Root/table-1` (blob_id, data0) VALUES(" + chunkNum + ", \"" + largeValue + "\");";
            
            KqpExecSync(runtime, query);
        }

        // Before compaction and changing the external threshold there should be no external blobs
        UNIT_ASSERT_VALUES_EQUAL(putCounter->PutsByChannel.size(), 0);

        auto schemeShardId = tableId1.PathId.OwnerId;

        auto req = CreateRequest(schemeShardId, 100, "/Root/", R"(
                            Name: "table-1"
                            PartitionConfig {
                             ColumnFamilies {
                               Id: 0
                               StorageConfig {
                                 ExternalThreshold: 300
                               }
                             }
                            })");
        
        runtime.SendToPipe(schemeShardId, sender, req.release(), 0, GetPipeConfigWithRetries());

        {
            runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvModifySchemeTransactionResult>(sender);

            auto evSubscribe = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(100);
            runtime.SendToPipe(schemeShardId, sender, evSubscribe.Release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto event = runtime.GrabEdgeEvent<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
            UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), 100);
        }
        
        CompactTable(runtime, shard1, tableId1, false);

        auto readFuture = KqpExec(runtime, "SELECT blob_id, data0 FROM `/Root/table-1`;");

        ValidateReadResult(runtime, std::move(readFuture), 512_B);

        // On compaction, inline blobs become external
        UNIT_ASSERT_VALUES_EQUAL(putCounter->PutsByChannel.size(), 2);
    }

    Y_UNIT_TEST(WithNewColumnFamilyAndCompaction) {
        TPortManager pm;

        TServerSettings serverSettings(pm.GetPort(2134));

        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .AddStoragePool("ssd")
            .AddStoragePool("hdd")
            .AddStoragePool("ext")
            .SetEnableUuidAsPrimaryKey(true);

        TServer::TPtr server = new TServer(serverSettings);
        
        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
    
        InitRoot(server, sender);
        
        TVector<TShardedTableOptions::TColumn> columns = {
                {"blob_id", "Int32", true, false},
                {"data", "String", false, false}
        };

        auto opts = TShardedTableOptions()
            .Columns(columns)
            .Families({{
                .Name = "default",
                .LogPoolKind = "ssd",
                .SysLogPoolKind = "ssd",
                .DataPoolKind = "ssd"
            }});

        CreateShardedTable(server, sender, "/Root", "table-1", opts);

        auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        TString largeValue(512_B, 'L');

        auto schemeShardId = tableId1.PathId.OwnerId;

        auto req = CreateRequest(schemeShardId, 100, "/Root/", R"(
                            Name: "table-1"
                            PartitionConfig {
                                ColumnFamilies {
                                    Id: 0
                                    StorageConfig {
                                        SysLog {
                                            PreferredPoolKind: "ssd"
                                        }
                                        Log {
                                            PreferredPoolKind: "ssd"
                                        }
                                        Data {
                                            PreferredPoolKind: "ssd"
                                        }
                                        External {
                                            PreferredPoolKind: "hdd"
                                            AllowOtherKinds: false
                                        }
                                        ExternalThreshold: 300
                                        ExternalChannelsCount: 3
                                    }
                                }
                                ColumnFamilies {
                                    Id: 1
                                    Name: "family_rot"
                                    StorageConfig {
                                        Data {
                                            PreferredPoolKind: "hdd"
                                            AllowOtherKinds: false
                                        }
                                    }
                                }
                            }
                            Columns {
                                Name: "data"
                                Id: 2
                                Family: 1
                                FamilyName: "family_rot"
                            })");
        
        runtime.SendToPipe(schemeShardId, sender, req.release(), 0, GetPipeConfigWithRetries());

        {
            runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvModifySchemeTransactionResult>(sender);

            auto evSubscribe = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(100);
            runtime.SendToPipe(schemeShardId, sender, evSubscribe.Release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto event = runtime.GrabEdgeEvent<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
            UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), 100);
        }

        auto putCounter = SetupPutCounter(runtime, 512_B);

        for (int i = 0; i < 100; i++) {
            TString chunkNum = ToString(i);
            TString query = "UPSERT INTO `/Root/table-1` (blob_id, data) VALUES(" + chunkNum + ", \"" + largeValue + "\");";

            KqpExecSync(runtime, query);
        }
        
        UNIT_ASSERT_VALUES_EQUAL(putCounter->PutsByChannel.size(), 3);

        putCounter->PutsByChannel.clear();
        
        CompactTable(runtime, shard1, tableId1, false);

        RebootTablet(runtime, shard1, sender);

        auto readFuture = KqpExec(runtime, "SELECT blob_id, data FROM `/Root/table-1`;");

        ValidateReadResult(runtime, std::move(readFuture), 512_B);

        UNIT_ASSERT_VALUES_EQUAL(putCounter->PutsByChannel.size(), 0);
    }

    Y_UNIT_TEST(ExtBlobsMultipleColumns) {
        TNode node(2);

        auto server = node.Server;
        auto& runtime = *node.Runtime;

        TString largeValue(2_KB, 'L');

        auto putCounter = SetupPutCounter(runtime);

        for (int i = 0; i < 100; i++) {
            TString chunkNum = ToString(i);
            TString query = "UPSERT INTO `/Root/table-1` (blob_id, data0, data1) VALUES(" + chunkNum + ", \"" + largeValue + "\", \"" + largeValue + "\");";

            KqpExecSync(runtime, query);
        }

        auto readFuture = KqpExec(runtime, "SELECT blob_id, data0, data1 FROM `/Root/table-1`;");

        ValidateReadResult(runtime, std::move(readFuture), 2_KB, 2);

        UNIT_ASSERT_VALUES_EQUAL(putCounter->PutsByChannel.size(), 2);
    }

    Y_UNIT_TEST(SingleChannel) {
        TNode node(1, 1);

        auto server = node.Server;
        auto& runtime = *node.Runtime;

        TString largeValue(2_KB, 'L');

        auto putCounter = SetupPutCounter(runtime);

        for (int i = 0; i < 100; i++) {
            TString chunkNum = ToString(i);
            TString query = "UPSERT INTO `/Root/table-1` (blob_id, data0) VALUES(" + chunkNum + ", \"" + largeValue + "\");";
            
            KqpExecSync(runtime, query);
        }

        auto readFuture = KqpExec(runtime, "SELECT blob_id, data0 FROM `/Root/table-1`;");

        ValidateReadResult(runtime, std::move(readFuture));

        UNIT_ASSERT_VALUES_EQUAL(putCounter->PutsByChannel.size(), 1);
    }

    std::vector<ui64> GetTabletGroups(TNode& node, ui64 hiveId, ui64 shardTabletId) {
        auto& runtime = *node.Runtime;
        auto& sender = node.Sender;

        TEvHive::TEvRequestHiveInfo::TRequestHiveInfoInitializer initializer = {
            .TabletId = shardTabletId,
            .ReturnChannelHistory = true,
        };

        runtime.SendToPipe(hiveId, sender, new TEvHive::TEvRequestHiveInfo(initializer), 0, GetPipeConfigWithRetries());

        auto hiveInfo = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(sender);

        std::vector<ui64> groups;

        for (auto& tabletInfo : hiveInfo->Get()->Record.GetTablets()) {
            if (tabletInfo.GetTabletID() == shardTabletId) {
                UNIT_ASSERT(tabletInfo.TabletChannelsSize() > 2); // first 2 channels are not external
                for (size_t i = 2; i < tabletInfo.TabletChannelsSize(); ++i) {
                    // Only get external data channels
                    auto& channelInfo = tabletInfo.GetTabletChannels(i);
                    auto& historyEl = channelInfo.GetHistory(channelInfo.HistorySize() - 1);
                    groups.push_back(historyEl.GetGroup());
                }
            }
        }

        return groups;
    }

    std::unique_ptr<TEvSchemeShard::TEvModifySchemeTransaction> ChangeExternalChannelsCountRequest(ui64 schemeShardId, ui32 externalChannelsCount, ui64 txId) {
        TString scheme = Sprintf(R"(
                Name: "table-1"
                PartitionConfig {
                    ColumnFamilies {
                        Id: 0
                        StorageConfig {
                            SysLog {
                                PreferredPoolKind: "ssd"
                            }
                            Log {
                                PreferredPoolKind: "ssd"
                            }
                            Data {
                                PreferredPoolKind: "ssd"
                            }
                            External {
                                PreferredPoolKind: "ext"
                                AllowOtherKinds: false
                            }
                            ExternalThreshold: 300
                            ExternalChannelsCount: %u
                        }
                    }
                    ColumnFamilies {
                        Id: 1
                        Name: "non_default"
                        StorageConfig {
                            Data {
                                PreferredPoolKind: "ssd"
                            }
                        }
                    }
                }
                Columns {
                    Name: "data0"
                    Id: 2
                    Family: 1
                    FamilyName: "non_default"
                })", externalChannelsCount);

        return CreateRequest(schemeShardId, txId, "/Root/", scheme);
    }

    Y_UNIT_TEST(ChangeExternalCount) {
        TNode node(1, 1);

        auto server = node.Server;
        auto& runtime = *node.Runtime;
        auto& sender = node.Sender;

        auto shard1 = node.Shard;
        auto tableId1 = node.TableId;

        ui64 hiveId = runtime.GetAppData(0).DomainsInfo->GetHive();
        ui64 schemeShardId = tableId1.PathId.OwnerId;

        auto groups1 = GetTabletGroups(node, hiveId, shard1);

        auto waitModify = [&](ui64 txId) {
            auto txRes = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvModifySchemeTransactionResult>(sender);
            auto status = txRes->Get()->Record.GetStatus();

            UNIT_ASSERT_VALUES_EQUAL(NKikimrScheme::StatusAccepted, status);

            auto evSubscribe = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(txId);
            runtime.SendToPipe(schemeShardId, sender, evSubscribe.Release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto event = runtime.GrabEdgeEvent<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
            UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), txId);
        };

        {
            auto req = ChangeExternalChannelsCountRequest(schemeShardId, 3, 100);
            runtime.SendToPipe(schemeShardId, sender, req.release(), 0, GetPipeConfigWithRetries());
            waitModify(100);
        }

        auto groups2 = GetTabletGroups(node, hiveId, shard1);

        {
            auto req = ChangeExternalChannelsCountRequest(schemeShardId, 2, 101);
            runtime.SendToPipe(schemeShardId, sender, req.release(), 0, GetPipeConfigWithRetries());
            waitModify(101);
        }

        auto groups3 = GetTabletGroups(node, hiveId, shard1);

        auto printGroups = [](const std::vector<ui64>& groups) {
            TStringStream ss;
            ss << "Groups: ";
            for (auto group : groups) {
                ss << group << " ";
            }
            ss << Endl;
            return ss.Str();
        };

        // Channels should be reused, so we expect the same groups
        UNIT_ASSERT_VALUES_EQUAL_C(groups1.size(), 1, printGroups(groups1));
        UNIT_ASSERT_VALUES_EQUAL_C(groups2.size(), 3, printGroups(groups2));
        UNIT_ASSERT_VALUES_EQUAL_C(groups3.size(), 3, printGroups(groups3));

        UNIT_ASSERT_VALUES_EQUAL(groups2, groups3);
    }

}

} // namespace NKikimr
