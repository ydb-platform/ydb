#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/client/flat_ut_client.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include "ydb_common_ut.h"

#include <util/thread/factory.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

using namespace NYdb;
using namespace NYdb::NTable;

void CreateTestTableWithIndex(NYdb::NTable::TTableClient& client) {
    auto sessionResult = client.CreateSession().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
    auto session = sessionResult.GetSession();

    const ui32 SHARD_COUNT = 4;

    {
        auto tableBuilder = client.GetTableBuilder()
                .AddNullableColumn("NameHash", EPrimitiveType::Uint32)
                .AddNullableColumn("Name", EPrimitiveType::Utf8)
                .AddNullableColumn("Version", EPrimitiveType::Uint32)
                .AddNullableColumn("Timestamp", EPrimitiveType::Int64)
                .AddNullableColumn("Data", EPrimitiveType::String)
                .SetPrimaryKeyColumns({"NameHash", "Name"})
                .AddSecondaryIndex("TimestampIndex",TVector<TString>({"Timestamp", "Name", "Version"}));

        auto tableSettings = NYdb::NTable::TCreateTableSettings().PartitioningPolicy(
            NYdb::NTable::TPartitioningPolicy().UniformPartitions(SHARD_COUNT));

        auto result = session.CreateTable("/Root/Foo", tableBuilder.Build(), tableSettings).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }
}

Y_UNIT_TEST_SUITE(YdbIndexTable) {
    Y_UNIT_TEST(FastSplitIndex) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);
        NFlatTests::TFlatMsgBusClient oldClient(server.ServerSettings->Port);

        CreateTestTableWithIndex(client);

        size_t shardsBefore = oldClient.GetTablePartitions("/Root/Foo/TimestampIndex/indexImplTable").size();
        Cerr << "Index table has " << shardsBefore << " shards" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(shardsBefore, 1);

        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);

        // Set low CPU usage threshold for robustness
        TAtomic unused;
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("SchemeShard_FastSplitCpuPercentageThreshold", 1, unused);
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("DataShardControls.CpuUsageReportThreshlodPercent", 1, unused);
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("DataShardControls.CpuUsageReportIntervalSeconds", 3, unused);

        TString query =
                "DECLARE $name_hash AS Uint32;\n"
                "DECLARE $name AS Utf8;\n"
                "DECLARE $version AS Uint32;\n"
                "DECLARE $timestamp AS Int64;\n\n"
                "UPSERT INTO `/Root/Foo` (NameHash, Name, Version, Timestamp) "
                "  VALUES ($name_hash, $name, $version, $timestamp);";

        TAtomic enough = 0;
        auto threadFunc = [&client, &query, &enough](TString namePrefix) {
            auto sessionResult = client.CreateSession().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
            auto session = sessionResult.GetSession();

            for (int key = 0 ; key < 2000 && !AtomicGet(enough); ++key) {
                TString name = namePrefix + ToString(key);

                auto paramsBuilder = client.GetParamsBuilder();
                auto params = paramsBuilder
                        .AddParam("$name_hash")
                            .Uint32(MurmurHash<ui32>(name.data(), name.size()))
                            .Build()
                        .AddParam("$name")
                            .Utf8(name)
                            .Build()
                        .AddParam("$version")
                            .Uint32(key%5)
                            .Build()
                        .AddParam("$timestamp")
                            .Int64(key%10)
                            .Build()
                    .Build();

                auto result = session.ExecuteDataQuery(
                            query,
                            TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();

                if (!result.IsSuccess() && result.GetStatus() != NYdb::EStatus::OVERLOADED) {
                    TString err = result.GetIssues().ToString();
                    Cerr << result.GetStatus() << ": " << err << Endl;
                }
                UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            }
        };

        IThreadFactory* pool = SystemThreadFactory();

        TAtomic finished = 0;
        TVector<TAutoPtr<IThreadFactory::IThread>> threads;
        threads.resize(10);
        for (size_t i = 0; i < threads.size(); i++) {
            TString namePrefix;
            namePrefix.append(5000, 'a' + i);
            threads[i] = pool->Run([threadFunc, namePrefix, &finished]() {
                threadFunc(namePrefix);
                AtomicIncrement(finished);
            });
        }

        // Wait for split to happen
        while (AtomicGet(finished) < (i64)threads.size()) {
            size_t shardsAfter = oldClient.GetTablePartitions("/Root/Foo/TimestampIndex/indexImplTable").size();
            if (shardsAfter > shardsBefore) {
                AtomicSet(enough, 1);
                break;
            }
            Sleep(TDuration::Seconds(5));
        }

        for (size_t i = 0; i < threads.size(); i++) {
            threads[i]->Join();
        }

        int retries = 5;
        size_t shardsAfter = 0;
        for (;retries > 0 && shardsAfter <= shardsBefore; --retries, Sleep(TDuration::Seconds(1))) {
            shardsAfter = oldClient.GetTablePartitions("/Root/Foo/TimestampIndex/indexImplTable").size();
        }
        Cerr << "Index table has " << shardsAfter << " shards" << Endl;
        UNIT_ASSERT_C(shardsAfter > shardsBefore, "Index table didn't split!!11 O_O");
    }

    Y_UNIT_TEST(AlterIndexImplBySuperUser) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);
        NFlatTests::TFlatMsgBusClient oldClient(server.ServerSettings->Port);

        CreateTestTableWithIndex(client);

        server.Server_->GetRuntime()->GetAppData().AdministrationAllowedSIDs.push_back("root@builtin");
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);

        {
            auto sessionResult = client.CreateSession().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
            auto session = sessionResult.GetSession();

            auto type = TTypeBuilder().BeginOptional().Primitive(EPrimitiveType::Uint64).EndOptional().Build();
            auto alter = TAlterTableSettings().AppendAddColumns(TColumn("FinishedTimestamp", type));

            auto alterResult = session.AlterTable("Root/Foo/TimestampIndex/indexImplTable", alter).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SCHEME_ERROR,
                                       "Alter of index impl table must fail");
        }

        {
            TAutoPtr<NMsgBusProxy::TBusResponse> result = oldClient.AlterTable("/Root/Foo/TimestampIndex", R"(
                Name: "indexImplTable"
                PartitionConfig {
                    PartitioningPolicy {
                        FastSplitSettings {
                            SizeThreshold: 10000
                            RowCountThreshold: 10000
                            CpuPercentageThreshold: 146
                        }
                    }
                }
            )", "user@builtin");
            UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_ERROR, "User must not be able to alter index impl table");
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetErrorReason(), "Administrative access denied");

            auto description = oldClient.Ls("/Root/Foo/TimestampIndex/indexImplTable");
            // Cerr << description->Record.GetPathDescription().GetTable().GetPartitionConfig() << Endl;
            UNIT_ASSERT(!description->Record.GetPathDescription().GetTable().GetPartitionConfig().GetPartitioningPolicy().HasFastSplitSettings());
        }

        {
            TAutoPtr<NMsgBusProxy::TBusResponse> result = oldClient.AlterTable("/Root/Foo/TimestampIndex", R"(
                Name: "indexImplTable"
                PartitionConfig {
                    PartitioningPolicy {
                        FastSplitSettings {
                            SizeThreshold: 10000
                            RowCountThreshold: 10000
                            CpuPercentageThreshold: 146
                        }
                    }
                }
            )", "root@builtin");
            UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK, "Super user must be able to alter partition config");

            auto description = oldClient.Ls("/Root/Foo/TimestampIndex/indexImplTable");
            // Cerr << description->Record.GetPathDescription().GetTable().GetPartitionConfig() << Endl;
            UNIT_ASSERT(description->Record.GetPathDescription().GetTable().GetPartitionConfig().GetPartitioningPolicy().HasFastSplitSettings());
            auto& fastSplitSettings = description->Record.GetPathDescription().GetTable().GetPartitionConfig().GetPartitioningPolicy().GetFastSplitSettings();
            UNIT_ASSERT_VALUES_EQUAL(fastSplitSettings.GetSizeThreshold(), 10000);
            UNIT_ASSERT_VALUES_EQUAL(fastSplitSettings.GetRowCountThreshold(), 10000);
            UNIT_ASSERT_VALUES_EQUAL(fastSplitSettings.GetCpuPercentageThreshold(), 146);
        }

        {
            TAutoPtr<NMsgBusProxy::TBusResponse> result = oldClient.AlterTable("/Root/Foo/TimestampIndex", R"(
                Name: "indexImplTable"
                PartitionConfig {
                    PartitioningPolicy {
                        SizeToSplit: 13000001
                    }
                }
            )", "root@builtin");
            UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK, "Super user must be able to alter partition config");

            auto description = oldClient.Ls("/Root/Foo/TimestampIndex/indexImplTable");
            // Cerr << description->Record.GetPathDescription().GetTable().GetPartitionConfig() << Endl;
            UNIT_ASSERT(description->Record.GetPathDescription().GetTable().GetPartitionConfig().GetPartitioningPolicy().HasFastSplitSettings());
            UNIT_ASSERT_VALUES_EQUAL(description->Record.GetPathDescription().GetTable().GetPartitionConfig().GetPartitioningPolicy().GetSizeToSplit(), 13000001);
        }

        {
            TAutoPtr<NMsgBusProxy::TBusResponse> result = oldClient.AlterTable("/Root/Foo/TimestampIndex", R"(
                Name: "indexImplTable"
                Columns {
                    Name: "NewColumn"
                    Type: "Uint32"
                }
            )", "root@builtin");
            UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_ERROR, "Super user must not be able to alter coloumns");
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetErrorReason(), "Adding or dropping columns in index table is not supported");
        }

        {
            TAutoPtr<NMsgBusProxy::TBusResponse> result = oldClient.AlterTable("/Root/Foo/TimestampIndex", R"(
                Name: "indexImplTable"
                DropColumns {
                    Name: "Timestamp"
                }
            )", "root@builtin");
            UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_ERROR, "Super user must not be able to alter coloumns");
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetErrorReason(), "Adding or dropping columns in index table is not supported");
        }
    }
}
