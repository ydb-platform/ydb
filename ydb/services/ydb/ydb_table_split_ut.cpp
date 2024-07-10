#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/client/flat_ut_client.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include "ydb_common_ut.h"

#include <util/thread/factory.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

using namespace NYdb;
using namespace NYdb::NTable;

void CreateTestTable(NYdb::NTable::TTableClient& client, const TString& name) {
    auto sessionResult = client.CreateSession().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
    auto session = sessionResult.GetSession();

    {
        auto query = TStringBuilder() << R"(
        --!syntax_v1
        CREATE TABLE `)" << name << R"(` (
            NameHash Uint32,
            Name Utf8,
            Version Uint32,
            `Timestamp` Int64,
            Data String,
            PRIMARY KEY (NameHash, Name)
        );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();

        Cerr << result.GetIssues().ToString();
        UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }
}

void SetAutoSplitByLoad(NYdb::NTable::TTableClient& client, const TString& tableName, bool enabled) {
    auto sessionResult = client.CreateSession().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
    auto session = sessionResult.GetSession();

    {
        auto query = TStringBuilder()
                << "--!syntax_v1\n"
                << "ALTER TABLE `" << tableName << "` "
                << "SET AUTO_PARTITIONING_BY_LOAD "
                << (enabled ? "ENABLED" : "DISABLED") << ";";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();

        Cerr << result.GetIssues().ToString();
        UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }
}

void RunQueryInLoop(NYdb::NTable::TTableClient& client, TString query, int keyCount, TAtomic& enough, TString namePrefix) {
    auto sessionResult = client.CreateSession().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
    auto session = sessionResult.GetSession();

    TExecDataQuerySettings querySettings;
    querySettings.KeepInQueryCache(true);

    for (int key = 0 ; key < keyCount && !AtomicGet(enough); ++key) {
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
                    TTxControl::BeginTx().CommitTx(),
                    std::move(params),
                    querySettings)
                .ExtractValueSync();

        if (!result.IsSuccess() && result.GetStatus() != NYdb::EStatus::OVERLOADED) {
            TString err = result.GetIssues().ToString();
            Cerr << result.GetStatus() << ": " << err << Endl;
        }
        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
    }
};

Y_UNIT_TEST_SUITE(YdbTableSplit) {
    void DoTestSplitByLoad(TKikimrWithGrpcAndRootSchema& server, TString query, bool fillWithData = false, size_t minSplits = 1) {
        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);
        NFlatTests::TFlatMsgBusClient oldClient(server.ServerSettings->Port);

        CreateTestTable(client, "/Root/Foo");

        if (fillWithData) {
            IThreadFactory* pool = SystemThreadFactory();

            TAtomic enough = 0;

            TVector<TAutoPtr<IThreadFactory::IThread>> threads;
            threads.resize(10);
            for (size_t i = 0; i < threads.size(); i++) {
                TString namePrefix = ToString(i) + "_";
                TString upsertQuery = R"(
                    DECLARE $name_hash AS Uint32;
                    DECLARE $name AS Utf8;
                    DECLARE $version AS Uint32;
                    DECLARE $timestamp AS Int64;

                    UPSERT INTO `/Root/Foo` (NameHash, Name, Version, Timestamp)
                    VALUES ($name_hash, $name, $version, $timestamp);
                )";
                threads[i] = pool->Run([&client, upsertQuery, &enough, namePrefix]() {
                    RunQueryInLoop(client, upsertQuery, 100, enough, namePrefix);
                });
            }
            for (size_t i = 0; i < threads.size(); i++) {
                threads[i]->Join();
            }
            Cerr << "Table filled with data" << Endl;
        }

        SetAutoSplitByLoad(client, "/Root/Foo", true);

        size_t shardsBefore = oldClient.GetTablePartitions("/Root/Foo").size();
        Cerr << "Table has " << shardsBefore << " shards" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(shardsBefore, 1);

        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_INFO);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_INFO);

        // Set low CPU usage threshold for robustness
        TAtomic unused;
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("SchemeShard_FastSplitCpuPercentageThreshold", 5, unused);
//        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("SchemeShard_SplitByLoadEnabled", 1, unused);
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("DataShardControls.CpuUsageReportThreshlodPercent", 1, unused);
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("DataShardControls.CpuUsageReportIntervalSeconds", 3, unused);

        TAtomic enough = 0;
        TAtomic finished = 0;

        IThreadFactory* pool = SystemThreadFactory();

        TVector<TAutoPtr<IThreadFactory::IThread>> threads;
        threads.resize(10);
        for (size_t i = 0; i < threads.size(); i++) {
            TString namePrefix = ToString(i) + "_";
            threads[i] = pool->Run([&client, &query, &enough, namePrefix, &finished]() {
                RunQueryInLoop(client, query, 100000, enough, namePrefix);
                AtomicIncrement(finished);
            });
        }

        // Wait for split to happen
        while (AtomicGet(finished) < (i64)threads.size()) {
            size_t shardsAfter = oldClient.GetTablePartitions("/Root/Foo").size();
            if (shardsAfter >= shardsBefore + minSplits) {
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
        for (;retries > 0 && !(shardsAfter >= shardsBefore + minSplits); --retries, Sleep(TDuration::Seconds(1))) {
            shardsAfter = oldClient.GetTablePartitions("/Root/Foo").size();
        }
        Cerr << "Table has " << shardsAfter << " shards" << Endl;
        UNIT_ASSERT_C(shardsAfter >= shardsBefore + minSplits, "Table didn't split!!11 O_O");
    }

    Y_UNIT_TEST(SplitByLoadWithReads) {
        TString query =
                "DECLARE $name_hash AS Uint32;\n"
                "DECLARE $name AS Utf8;\n"
                "DECLARE $version AS Uint32;\n"
                "DECLARE $timestamp AS Int64;\n\n"
                "SELECT * FROM `/Root/Foo` \n"
                "WHERE NameHash = $name_hash AND Name = $name";

        TKikimrWithGrpcAndRootSchema server;
        DoTestSplitByLoad(server, query);
    }

    Y_UNIT_TEST(SplitByLoadWithReadsMultipleSplitsWithData) {
        TString query =
                "DECLARE $name_hash AS Uint32;\n"
                "DECLARE $name AS Utf8;\n"
                "DECLARE $version AS Uint32;\n"
                "DECLARE $timestamp AS Int64;\n\n"
                "SELECT * FROM `/Root/Foo` \n"
                "WHERE NameHash = $name_hash AND Name = $name";

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(false);
        TKikimrWithGrpcAndRootSchema server(appConfig);
        DoTestSplitByLoad(server, query, /* fill with data */ true, /* at least two splits */ 2);
    }

    Y_UNIT_TEST(SplitByLoadWithUpdates) {
        TString query =
                "DECLARE $name_hash AS Uint32;\n"
                "DECLARE $name AS Utf8;\n"
                "DECLARE $version AS Uint32;\n"
                "DECLARE $timestamp AS Int64;\n\n"
                "UPSERT INTO `/Root/Foo` (NameHash, Name, Version, Timestamp) "
                "  VALUES ($name_hash, $name, $version, $timestamp);";

        TKikimrWithGrpcAndRootSchema server;
        DoTestSplitByLoad(server, query);
    }

    Y_UNIT_TEST(SplitByLoadWithDeletes) {
        TString query =
                "DECLARE $name_hash AS Uint32;\n"
                "DECLARE $name AS Utf8;\n"
                "DECLARE $version AS Uint32;\n"
                "DECLARE $timestamp AS Int64;\n\n"
                "DELETE FROM `/Root/Foo` \n"
                "WHERE NameHash = $name_hash AND Name = $name";

        TKikimrWithGrpcAndRootSchema server;
        DoTestSplitByLoad(server, query);
    }

    Y_UNIT_TEST(SplitByLoadWithNonEmptyRangeReads) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);
        NFlatTests::TFlatMsgBusClient oldClient(server.ServerSettings->Port);

        CreateTestTable(client, "/Root/Foo");

        // Fill the table with some data
        {
            TString upsertQuery =
                    "DECLARE $name_hash AS Uint32;\n"
                    "DECLARE $name AS Utf8;\n"
                    "DECLARE $version AS Uint32;\n"
                    "DECLARE $timestamp AS Int64;\n\n"
                    "UPSERT INTO `/Root/Foo` (NameHash, Name, Version, Timestamp) "
                    "  VALUES ($name_hash, $name, $version, $timestamp);";

            TAtomic enough = 0;
            RunQueryInLoop(client, upsertQuery, 2000, enough, "");
        }

        SetAutoSplitByLoad(client, "/Root/Foo", true);

        // Set low CPU usage threshold for robustness
        TAtomic unused;
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("SchemeShard_FastSplitCpuPercentageThreshold", 5, unused);
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("DataShardControls.CpuUsageReportThreshlodPercent", 1, unused);
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("DataShardControls.CpuUsageReportIntervalSeconds", 3, unused);

        size_t shardsBefore = oldClient.GetTablePartitions("/Root/Foo").size();
        Cerr << "Table has " << shardsBefore << " shards" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(shardsBefore, 1);

        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_INFO);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_INFO);

        TString rangeQuery =
                "DECLARE $name_hash AS Uint32;\n"
                "DECLARE $name AS Utf8;\n"
                "DECLARE $version AS Uint32;\n"
                "DECLARE $timestamp AS Int64;\n\n"
                "SELECT count(*) FROM `/Root/Foo` \n"
                "WHERE NameHash = $name_hash;";

        TAtomic enough = 0;
        TAtomic finished = 0;

        IThreadFactory* pool = SystemThreadFactory();

        TVector<TAutoPtr<IThreadFactory::IThread>> threads;
        threads.resize(10);
        for (size_t i = 0; i < threads.size(); i++) {
            threads[i] = pool->Run([&client, &rangeQuery, &enough, &finished]() {
                for (int i = 0; i < 10; ++i) {
                    RunQueryInLoop(client, rangeQuery, 2000, enough, "");
                }
                AtomicIncrement(finished);
            });
        }

        // Wait for split to happen
        while (AtomicGet(finished) < (i64)threads.size()) {
            size_t shardsAfter = oldClient.GetTablePartitions("/Root/Foo").size();
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
            shardsAfter = oldClient.GetTablePartitions("/Root/Foo").size();
        }
        Cerr << "Table has " << shardsAfter << " shards" << Endl;
        UNIT_ASSERT_C(shardsAfter > shardsBefore, "Table didn't split!!11 O_O");
    }

    // Allows to adjust the time
    class TTestTimeProvider : public ITimeProvider {
        TIntrusivePtr<ITimeProvider> RealProvider;
        TAtomic Shift;

    public:
        explicit TTestTimeProvider(TIntrusivePtr<ITimeProvider> realProvider)
            : RealProvider(realProvider)
            , Shift(0)
        {}

        TInstant Now() override {
            return RealProvider->Now() + TDuration::MicroSeconds(AtomicGet(Shift));
        }

        void AddShift(TDuration delta) {
            AtomicAdd(Shift, delta.MicroSeconds());
        }
    };

    Y_UNIT_TEST(MergeByNoLoadAfterSplit) {
        // Create test table and read many rows and trigger split
        TString query =
                "DECLARE $name_hash AS Uint32;\n"
                "DECLARE $name AS Utf8;\n"
                "DECLARE $version AS Uint32;\n"
                "DECLARE $timestamp AS Int64;\n\n"
                "SELECT * FROM `/Root/Foo` \n"
                "WHERE NameHash = $name_hash AND Name = $name";

        TIntrusivePtr<ITimeProvider> originalTimeProvider = NKikimr::TAppData::TimeProvider;
        TIntrusivePtr<TTestTimeProvider> testTimeProvider = new TTestTimeProvider(originalTimeProvider);
        NKikimr::TAppData::TimeProvider = testTimeProvider;

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(false);
        TKikimrWithGrpcAndRootSchema server(appConfig);

        // Set min uptime before merge by load to 10h
        TAtomic unused;
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("SchemeShard_MergeByLoadMinUptimeSec", 4*3600, unused);
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("SchemeShard_MergeByLoadMinLowLoadDurationSec", 10*3600, unused);

        Cerr << "Triggering split by load" << Endl;
        DoTestSplitByLoad(server, query);

        // Set split threshold very high and run some more load on new shards
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("SchemeShard_FastSplitCpuPercentageThreshold", 110, unused);

        Cerr << "Loading new shards" << Endl;
        {
            NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
            NYdb::NTable::TTableClient client(driver);
            TInstant startTime = testTimeProvider->Now();
            while (testTimeProvider->Now() < startTime + TDuration::Seconds(20)) {
                TAtomic enough = 0;
                RunQueryInLoop(client, query, 1000, enough, "");
            }
        }

        // Set split threshold at 10% so that merge can be trigger after high load goes away
        server.Server_->GetRuntime()->GetAppData().Icb->SetValue("SchemeShard_FastSplitCpuPercentageThreshold", 10, unused);

        // Stop all load an see how many partitions the table has
        NFlatTests::TFlatMsgBusClient oldClient(server.ServerSettings->Port);
        size_t shardsBefore = oldClient.GetTablePartitions("/Root/Foo").size();
        Cerr << "Table has " << shardsBefore << " shards" << Endl;
        UNIT_ASSERT_VALUES_UNEQUAL(shardsBefore, 1);

        // Fast forward time a bit multiple time and check that merge doesn't happen
        /*
        for (int i = 0; i < 7; ++i) {
            Cerr << "Fast forward 1h" << Endl;
            testTimeProvider->AddShift(TDuration::Hours(1));
            Sleep(TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL(oldClient.GetTablePartitions("/Root/Foo").size(), shardsBefore);
        }*/

        Cerr << "Fast forward > 10h to trigger the merge" << Endl;
        testTimeProvider->AddShift(TDuration::Hours(16));

        // Wait for merge to happen
        size_t shardsAfter = shardsBefore;
        for (int i = 0; i < 20; ++i) {
            size_t shardsAfter = oldClient.GetTablePartitions("/Root/Foo").size();
            if (shardsAfter < shardsBefore) {
                return;
            }
            Sleep(TDuration::Seconds(5));
        }
        UNIT_ASSERT_C(shardsAfter < shardsBefore, "Merge didn't happen!!11 O_O");
    }

    Y_UNIT_TEST(RenameTablesAndSplit) {
        // KIKIMR-14636

        NDataShard::gDbStatsReportInterval = TDuration::Seconds(2);
        NDataShard::gDbStatsDataSizeResolution = 10;
        NDataShard::gDbStatsRowCountResolution = 10;

        TIntrusivePtr<ITimeProvider> originalTimeProvider = NKikimr::TAppData::TimeProvider;
        TIntrusivePtr<TTestTimeProvider> testTimeProvider = new TTestTimeProvider(originalTimeProvider);
        NKikimr::TAppData::TimeProvider = testTimeProvider;

        TKikimrWithGrpcAndRootSchemaNoSystemViews server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_NOTICE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_NOTICE);

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session = sessionResult.GetSession();

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `/Root/Foo` (
                NameHash Uint32,
                Name Utf8,
                Version Uint32,
                `Timestamp` Int64,
                Data String,
                PRIMARY KEY (NameHash, Name)
            ) WITH ( UNIFORM_PARTITIONS = 2 );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();

            Cerr << result.GetIssues().ToString();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        { // prepare for split
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE  `/Root/Foo`
            SET (
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 1,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 10
            );)";

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();

            Cerr << result.GetIssues().ToString();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        ui64 partitions = 2;
        do { // wait until merge
            Cerr << "Fast forward 1m" << Endl;
            testTimeProvider->AddShift(TDuration::Minutes(2));
            Sleep(TDuration::Seconds(3));

            auto result = session.DescribeTable("/Root/Foo", NYdb::NTable::TDescribeTableSettings().WithTableStatistics(true)).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            partitions = result.GetTableDescription().GetPartitionsCount();
            Cerr << "partitions " << partitions << Endl;

        } while (partitions == 2);

        { //rename
            auto result = session.RenameTables({{"/Root/Foo", "/Root/Bar"}}).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        { // add data for triger split
            int key = 0;
            for (int i = 0 ; i < 100; ++i) {
                TValueBuilder rows;
                rows.BeginList();
                for (int j = 0; j < 500; ++j) {
                    key += 1;
                    TString name = "key " + ToString(key);

                    rows.AddListItem()
                        .BeginStruct()
                            .AddMember("NameHash").Uint32(MurmurHash<ui32>(name.data(), name.size()))
                            .AddMember("Name").Utf8(name)
                            .AddMember("Version").Uint32(key%5)
                            .AddMember("Timestamp").Int64(key%10)
                        .EndStruct();
                }
                rows.EndList();

                auto result = client.BulkUpsert("/Root/Bar", rows.Build()).ExtractValueSync();

                if (!result.IsSuccess() && result.GetStatus() != NYdb::EStatus::OVERLOADED) {
                    TString err = result.GetIssues().ToString();
                    Cerr << result.GetStatus() << ": " << err << Endl;
                }
                UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            }
        }

        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);

        partitions = 1;
        do { // wait until split
            Cerr << "Fast forward 1m" << Endl;
            testTimeProvider->AddShift(TDuration::Minutes(1));
            Sleep(TDuration::Seconds(3));

            auto result = session.DescribeTable("/Root/Bar", NYdb::NTable::TDescribeTableSettings().WithTableStatistics(true)).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            partitions = result.GetTableDescription().GetPartitionsCount();
            Cerr << "partitions " << partitions << Endl;

        } while (partitions == 1);


        { // fail if shema has been broken
            TString readQuery =
                    "SELECT * FROM `/Root/Bar`;";

            TExecDataQuerySettings querySettings;
            querySettings.KeepInQueryCache(true);

            auto result = session.ExecuteDataQuery(
                        readQuery,
                        TTxControl::BeginTx().CommitTx(),
                        querySettings)
                    .ExtractValueSync();

            if (!result.IsSuccess() && result.GetStatus() != NYdb::EStatus::OVERLOADED) {
                TString err = result.GetIssues().ToString();
                Cerr << result.GetStatus() << ": " << err << Endl;
            }
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto asyncDescDir = NYdb::NScheme::TSchemeClient(connection).ListDirectory("/Root");
            asyncDescDir.Wait();
            const auto& val = asyncDescDir.GetValue();
            auto entry = val.GetEntry();
            UNIT_ASSERT_EQUAL(entry.Name, "Root");
            UNIT_ASSERT_EQUAL(entry.Type, NYdb::NScheme::ESchemeEntryType::Directory);

            auto children = val.GetChildren();
            UNIT_ASSERT_EQUAL_C(children.size(), 1, children.size());
            for (const auto& child: children) {
                UNIT_ASSERT_EQUAL(child.Type, NYdb::NScheme::ESchemeEntryType::Table);

                auto result = session.DropTable(TStringBuilder() << "Root" << "/" <<  child.Name).ExtractValueSync();
                UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetStatus());
            }
        }
    }
}
