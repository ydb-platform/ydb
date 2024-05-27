#include "workload.h"

#include <ydb/library/workload/tpcc/error.h>
#include <ydb/library/workload/tpcc/terminal.h>
#include <ydb/library/workload/tpcc/load_task.h>
#include <ydb/library/workload/tpcc/load_data/customer.h>
#include <ydb/library/workload/tpcc/load_data/district.h>
#include <ydb/library/workload/tpcc/load_data/history.h>
#include <ydb/library/workload/tpcc/load_data/item.h>
#include <ydb/library/workload/tpcc/load_data/new_order.h>
#include <ydb/library/workload/tpcc/load_data/oorder.h>
#include <ydb/library/workload/tpcc/load_data/order_line.h>
#include <ydb/library/workload/tpcc/load_data/stock.h>
#include <ydb/library/workload/tpcc/load_data/warehouse.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>

#include <util/string/printf.h>
#include <library/cpp/logger/stream_creator.h>


using namespace NYdb;

namespace NYdbWorkload {
namespace NTPCC {

TTPCCWorkload::TTPCCWorkload(std::shared_ptr<TDriver>& driver)
    : Driver(driver)
    , Log(TCoutLogBackendCreator().CreateLogBackend())
    , Seed(Now().MicroSeconds())
{
    Log.SetFormatter([](ELogPriority priority, TStringBuf message) -> TString {
        TStringStream stream;
        stream << TInstant::Now().ToRfc822String() << ": ";
        if (priority == TLOG_ERR) {
            stream << ToString(NColorizer::RED) << priority << ToString(NColorizer::RESET);
        } else {
            stream << priority;
        }
        stream << ": " << message << Endl;
        return std::move(stream.Str());
    });
}

void TTPCCWorkload::SetLoadParams(const TLoadParams& params) {
    LoadParams = params;
}

void TTPCCWorkload::SetRunParams(const TRunParams& params) {
    RunParams = params;
}

TString TTPCCWorkload::CreateTablesQuery() {
    TStringStream query;
    query << "--!syntax_v1";
    TString whParts = PartitionByWarehouses(LoadParams.Warehouses, LoadParams.Threads);
    query << TWarehouseLoadQueryGenerator::GetCreateDDL(TString(LoadParams.DbPath));
    query << TItemLoadQueryGenerator::GetCreateDDL(TString(LoadParams.DbPath));
    query << TDistrictLoadQueryGenerator::GetCreateDDL(TString(LoadParams.DbPath));
    query << TStockLoadQueryGenerator::GetCreateDDL(TString(LoadParams.DbPath), LoadParams.Threads, whParts);
    query << TCustomerLoadQueryGenerator::GetCreateDDL(TString(LoadParams.DbPath), LoadParams.Threads, whParts);
    query << TOorderLoadQueryGenerator::GetCreateDDL(TString(LoadParams.DbPath), LoadParams.Threads, whParts);
    query << TOrderLineLoadQueryGenerator::GetCreateDDL(TString(LoadParams.DbPath), LoadParams.Threads, whParts);
    query << TNewOrderLoadQueryGenerator::GetCreateDDL(TString(LoadParams.DbPath), LoadParams.Threads, whParts);
    query << THistoryLoadQueryGenerator::GetCreateDDL(TString(LoadParams.DbPath), LoadParams.Threads, whParts);
    return query.Str();
}

TString TTPCCWorkload::PartitionByWarehouses(i32 whs, i32 partsCount) {
    TStringStream partition;
    for (i32 i = 1; i < partsCount; i++) {
        partition << i * whs / partsCount;
        if (i != partsCount - 1) {
            partition << ", ";
        }
    }
    return partition.Str();
}

TString TTPCCWorkload::CleanTablesQuery() {
    TStringStream query;
    query << THistoryLoadQueryGenerator::GetCleanDDL();
    query << TNewOrderLoadQueryGenerator::GetCleanDDL();
    query << TOrderLineLoadQueryGenerator::GetCleanDDL();
    query << TOorderLoadQueryGenerator::GetCleanDDL();
    query << TCustomerLoadQueryGenerator::GetCleanDDL();
    query << TDistrictLoadQueryGenerator::GetCleanDDL();
    query << TStockLoadQueryGenerator::GetCleanDDL();
    query << TItemLoadQueryGenerator::GetCleanDDL();
    query << TWarehouseLoadQueryGenerator::GetCleanDDL();
    return query.Str();
}

int TTPCCWorkload::InitTables() {
    std::shared_ptr<NTable::TTableClient> tableClient(new NTable::TTableClient(*Driver));

    try {
        if (LoadParams.OnlyConfiguring) {
            ConfigurePartitioning();
            
            return EXIT_SUCCESS;
        }

        if (LoadParams.OnlyCreatingIndices) {
            CreateIndices();
            
            return EXIT_SUCCESS;
        }

        if (LoadParams.Seed > 0) {
            Seed = LoadParams.Seed;
        }
        Rng.ConstructInPlace(Seed);
        
        Log.Write(TLOG_INFO, "Seed: " + std::to_string(Seed));
        
        // Create Tables
        Log.Write(TLOG_INFO, "Creating tables...");
        
        auto sessionResult = tableClient->GetSession().GetValueSync();
        ThrowOnError(sessionResult, Log, __func__);
        auto result = sessionResult.GetSession().ExecuteSchemeQuery(CreateTablesQuery()).GetValueSync();
        ThrowOnError(result, Log, __func__);
        
        Log.Write(TLOG_INFO, "DONE");
        
        Log.Write(TLOG_INFO, "Loading data...");

        auto started = Now();

        // Load Data

        std::vector<std::atomic_int32_t> finished(9);
        std::vector<std::atomic_bool> failed(9);
        std::vector<ETablesType> loadOrder;
        
        ThreadPool = std::make_shared<TThreadPool>();

        ThreadPool->Start(LoadParams.Threads);

        ThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
            std::make_unique<TWarehouseLoadQueryGenerator>(LoadParams, Log, Rng->GenRand()),
            tableClient, finished[0], failed[0], Log
        ));
        loadOrder.push_back(ETablesType::warehouse);

        ThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
            std::make_unique<TItemLoadQueryGenerator>(LoadParams, Log, Rng->GenRand()),
            tableClient, finished[1], failed[1], Log
        ));
        loadOrder.push_back(ETablesType::item);

        ThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
            std::make_unique<TDistrictLoadQueryGenerator>(LoadParams, Log, Rng->GenRand()),
            tableClient, finished[2], failed[2], Log
        ));
        loadOrder.push_back(ETablesType::district);

        for (i32 threadNum = 1; threadNum <= LoadParams.Threads; ++threadNum) {
            ThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<TCustomerLoadQueryGenerator>(LoadParams, threadNum, Log, Rng->GenRand()),
                tableClient, finished[3], failed[3], Log
            ));
        }
        loadOrder.push_back(ETablesType::customer);

        for (i32 threadNum = 1; threadNum <= LoadParams.Threads; ++threadNum) {
            ThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<THistoryLoadQueryGenerator>(LoadParams, threadNum, Log, Rng->GenRand()),
                tableClient, finished[4], failed[4], Log
            ));
        }
        loadOrder.push_back(ETablesType::history);

        for (i32 threadNum = 1; threadNum <= LoadParams.Threads; ++threadNum) {
            ThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<TNewOrderLoadQueryGenerator>(LoadParams, threadNum, Log, Rng->GenRand()),
                tableClient, finished[5], failed[5], Log
            ));
        }
        loadOrder.push_back(ETablesType::new_order);

        for (i32 threadNum = 1; threadNum <= LoadParams.Threads; ++threadNum) {
            ThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<TOorderLoadQueryGenerator>(LoadParams, threadNum, Log, Rng->GenRand()),
                tableClient, finished[6], failed[6], Log
            ));
        }
        loadOrder.push_back(ETablesType::oorder);
        
        for (i32 threadNum = 1; threadNum <= LoadParams.Threads; ++threadNum) {
            ThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<TStockLoadQueryGenerator>(LoadParams, threadNum, Log, Rng->GenRand()),
                tableClient, finished[7], failed[7], Log
            ));
        }
        loadOrder.push_back(ETablesType::stock);

        for (i32 threadNum = 1; threadNum <= LoadParams.Threads; ++threadNum) {
            ThreadPool->SafeAddAndOwn(MakeHolder<TLoadTask>(
                std::make_unique<TOrderLineLoadQueryGenerator>(LoadParams, threadNum, Log, Rng->GenRand()),
                tableClient, finished[8], failed[8], Log
            ));
        }
        loadOrder.push_back(ETablesType::order_line);
        
        ThreadPool->Stop();

        Log.Write(TLOG_INFO, "Duration: " + (Now() - started).ToString());

        bool loadFailed = false;
        for (i32 index = 0; index < 9; ++index) {
            if (failed[index]) {
                loadFailed = true;
                break;
            }
        }

        if (loadFailed) {
            Log.Write(TLOG_INFO, "FAILED");
            return EXIT_FAILURE;
        }

        Log.Write(TLOG_INFO, "DONE");

        int exitStatus = ConfigurePartitioning();
        if (exitStatus == EXIT_FAILURE) {
            return EXIT_FAILURE;
        }

        exitStatus = CreateIndices();
        if (exitStatus == EXIT_FAILURE) {
            return EXIT_FAILURE;
        }

        tableClient->Stop();
    } catch (const TErrorException& ex) {
        tableClient->Stop();
        TStringStream stream;
        stream << "Init tables: " << ex;
        Log.Write(TLOG_ERR, stream.Str());
        return EXIT_FAILURE;
    } catch (const std::exception& ex) {
        tableClient->Stop();
        TStringStream stream;
        stream << "Init tables: " << ex.what();
        Log.Write(TLOG_ERR, stream.Str());
        return EXIT_FAILURE;
    } catch (...) {
        tableClient->Stop();
        Log.Write(TLOG_ERR, "Failed with an unknown error");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

int TTPCCWorkload::CleanTables() {
    try {
        auto db = NTable::TTableClient(*Driver);
        auto sessionResult = db.GetSession().GetValueSync();
        ThrowOnError(sessionResult, Log, __func__);
        auto result = sessionResult.GetSession().ExecuteSchemeQuery(CleanTablesQuery()).GetValueSync();
        ThrowOnError(result, Log, __func__);
    } catch (const TErrorException& ex) {
        TStringStream stream;
        stream<< "Clean tables: "  << ex;
        Log.Write(TLOG_ERR, stream.Str());
        return EXIT_FAILURE;
    } catch (const std::exception& ex) {
        TStringStream stream;
        stream<< "Clean tables: "  << ex.what();
        Log.Write(TLOG_ERR, stream.Str());
        return EXIT_FAILURE;
    } catch (...) {
        Log.Write(TLOG_ERR, "Failed with an unknown error");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

int TTPCCWorkload::ConfigurePartitioning() {
    Log.Write(TLOG_INFO, "Configuring partitioning...");
    
    static const std::vector<TString> heavyTables {
        "customer", "history", "new_order",
        "oorder", "order_line", "stock"
    };

    auto tableClientSettings = NTable::TClientSettings()
                        .SessionPoolSettings(
                            NTable::TSessionPoolSettings()
                                .MaxActiveSessions(heavyTables.size()));
    NTable::TTableClient tableClient(*Driver, tableClientSettings);

    try {
        TString query;
        for (auto table: heavyTables) {
            query += Sprintf(R"(
                ALTER TABLE `%s` SET (
                    AUTO_PARTITIONING_BY_LOAD = %s,
                    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %s
                );
            )", table.c_str(), LoadParams.DisableAutoPartitioning ? "DISABLED" : "ENABLED"
            , ToString(LoadParams.MaxPartitions).c_str());
        }

        std::vector<NTable::TSession> sessions;
        NTable::TCreateSessionResult sessionResult = tableClient.GetSession().GetValueSync();
        ThrowOnError(sessionResult, Log, __func__);
        sessions.push_back(sessionResult.GetSession());

        TStatus result = sessions[0].ExecuteSchemeQuery(query.c_str()).GetValueSync();
        ThrowOnError(result, Log, "Configuring partitioning failed with issues");

        for (ui32 i = 1; i < heavyTables.size(); ++i) {
            NTable::TCreateSessionResult sessionResult = tableClient.GetSession().GetValueSync();
            ThrowOnError(sessionResult, Log, __func__);
            sessions.push_back(sessionResult.GetSession());
        }

        std::vector<i32> prevPartition(heavyTables.size());
        std::vector<NThreading::TFuture<void>> toWait;
        
        Sleep(TDuration::Seconds(60));

        while (true) {
            toWait.clear();

            std::atomic_bool done = true;
            for (ui32 i = 0; i < heavyTables.size(); ++i) {
                NThreading::TFuture<void> future = sessions[i].DescribeTable(
                    LoadParams.DbPath + "/" + heavyTables[i],
                    NTable::TDescribeTableSettings().WithTableStatistics(true)
                ).Apply(
                    [&prevPartition, i, &done](const NTable::TAsyncDescribeTableResult& future) {
                        NTable::TDescribeTableResult result = future.GetValueSync();

                        if (!result.IsSuccess()) {
                            throw yexception() << "Getting describe table failed with issues: " << result.GetIssues().ToString();
                        }

                        i32 partCount = result.GetTableDescription().GetPartitionsCount();
                        if (prevPartition[i] != partCount) {
                            prevPartition[i] = partCount;
                            done.store(false);
                        }
                    }
                );

                toWait.emplace_back(std::move(future));
            }

            NThreading::WaitExceptionOrAll(toWait).GetValueSync();
            
            if (done.load()) {
                break;
            } else {
                Sleep(TDuration::Seconds(60));
            }
        }
    } catch (const std::exception& ex) {
        tableClient.Stop();

        TStringStream stream;
        stream<< "Describe table: "  << ex.what();
        Log.Write(TLOG_ERR, stream.Str());

        Log.Write(TLOG_INFO, "FAILED");
        return EXIT_FAILURE;
    }

    tableClient.Stop();

    Log.Write(TLOG_INFO, "DONE");

    return EXIT_SUCCESS;
}

int TTPCCWorkload::CreateIndices() {
    Log.Write(TLOG_INFO, "Creating indices...");

    std::vector<std::pair<std::string, std::string>> indices {{"idx_customer_name", R"(
        --!syntax_v1
        ALTER TABLE `customer` ADD INDEX `idx_customer_name` GLOBAL ON (C_W_ID, C_D_ID, C_LAST, C_FIRST);
    )"}, {"idx_order", R"(
        --!syntax_v1
        ALTER TABLE `oorder` ADD INDEX `idx_order` GLOBAL ON (O_W_ID, O_D_ID, O_C_ID, O_ID);
    )"}};
    
    auto tableClientSettings = NTable::TClientSettings()
                        .SessionPoolSettings(
                            NTable::TSessionPoolSettings()
                                .MaxActiveSessions(indices.size()));
    NTable::TTableClient tableClient(*Driver, tableClientSettings);

    try {
        std::vector<NTable::TSession> sessions;
        std::vector<TAsyncStatus> futures;
        for (auto index: indices) {
            NTable::TCreateSessionResult result = tableClient.GetSession().GetValueSync();
            ThrowOnError(static_cast<TStatus>(result), Log, __func__);
            sessions.push_back(result.GetSession());
            futures.push_back(sessions.back().ExecuteSchemeQuery(index.second.c_str()));
        }

        WaitExceptionOrAll(futures).GetValueSync();

        for (ui32 i = 0; i < indices.size(); i++) {
            auto status = futures[i].GetValueSync();
            if (!status.IsSuccess()) {
                throw yexception() << "Failed to create index `" << indices[i].first <<  "`: " << status.GetIssues().ToString();
            }
        }
    } catch (const std::exception& ex) {
        tableClient.Stop();

        TStringStream stream;
        stream<< "Describe table: "  << ex.what();
        Log.Write(TLOG_ERR, stream.Str());

        Log.Write(TLOG_INFO, "FAILED");
        return EXIT_FAILURE;
    }

    tableClient.Stop();

    Log.Write(TLOG_INFO, "DONE");

    return EXIT_SUCCESS;
}

void TTPCCWorkload::CollectAndPrintResult() {
    std::vector<std::unique_ptr<TStatistics>> statistics;

    for (i32 procedure = 0; procedure < EProcedureType::COUNT; procedure++) {
        auto stats = std::make_unique<TStatistics>(60000, 2);
        
        for (auto& terminal: Terminals) {
            auto& terminalStats = terminal->GetStatistics();
        
            stats->Add(terminalStats[procedure]);
        }

        statistics.emplace_back(std::move(stats));
    }

    double tpmc = 1.0 * statistics[EProcedureType::NewOrder]->Successes * 60 / RunParams.DurationS;
    double efficiency = tpmc * 100 / RunParams.Warehouses / 12.86;

    TStringStream stream;
    stream << Sprintf("Duration, s: %d, Warehouses: %d, Efficiency: %.2f, tpmc: %.2f",
                      RunParams.DurationS, RunParams.Warehouses, efficiency, tpmc) << Endl;
    stream << "___ops/sec___successes___(%)______failes_____retries__p50(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)" << Endl;

    std::unordered_map<EStatus, std::vector<i32>> retriesWithStatus;
    std::unordered_map<EStatus, std::vector<i32>> failesWithStatus;

    for (i32 procedure = 0; procedure < EProcedureType::COUNT; procedure++) {
        stream << Sprintf("%10.1f %11ld %5.1f %11ld %11ld %8ld %8ld %8ld %8ld %8ld %s\n",
            1.0 * statistics[procedure]->Successes / RunParams.DurationS,
            statistics[procedure]->Successes,
            100.0 * statistics[procedure]->Successes / (statistics[procedure]->Successes + statistics[procedure]->Failes),
            statistics[procedure]->Failes,
            statistics[procedure]->Retries,
            statistics[procedure]->Hist.GetValueAtPercentile(50),
            statistics[procedure]->Hist.GetValueAtPercentile(90),
            statistics[procedure]->Hist.GetValueAtPercentile(95),
            statistics[procedure]->Hist.GetValueAtPercentile(99),
            statistics[procedure]->Hist.GetMax(),
            ToString(static_cast<EProcedureType>(procedure)).c_str()
        );

        for (auto [status, retries]: statistics[procedure]->RetriesWithStatus) {
            if (!retriesWithStatus.contains(status)) {
                retriesWithStatus[status] = std::vector<i32>(EProcedureType::COUNT, 0);
            }
            
            retriesWithStatus[status][procedure] += retries;
        }
        
        for (auto [status, failes]: statistics[procedure]->FailesWithStatus) {
            if (!failesWithStatus.contains(status)) {
                failesWithStatus[status] = std::vector<i32>(EProcedureType::COUNT, 0);
            }
            
            failesWithStatus[status][procedure] += failes;
        }
    }

    stream << Endl;

    for (i32 procedure = 0; procedure < EProcedureType::COUNT; procedure++) {
        stream << ToString(static_cast<EProcedureType>(procedure)).c_str() << Endl;
        stream << "_________EStatus_________   __Retries__   __Failes__ " << Endl;
        
        std::set<EStatus> statuses;
        for (auto [status, _]: statistics[procedure]->RetriesWithStatus) {
            statuses.insert(status);
        }
        for (auto [status, _]: statistics[procedure]->FailesWithStatus) {
            statuses.insert(status);
        }

        for (auto status: statuses) {
            stream << Sprintf(
                "%25s   %11d   %10d\n",
                ToString(static_cast<EStatus>(status)).c_str(),
                retriesWithStatus.contains(status) ? retriesWithStatus[status][procedure] : 0,
                failesWithStatus.contains(status) ? failesWithStatus[status][procedure] : 0
            );
        }
        stream << Endl;
    }

    Cout << stream.Str() << Endl;
}

int TTPCCWorkload::RunWorkload() {
    auto tableClientSettings = NTable::TClientSettings()
                        .SessionPoolSettings(
                            NTable::TSessionPoolSettings()
                                .MaxActiveSessions(RunParams.MaxActiveSessions));

    std::shared_ptr<NTable::TTableClient> tableClient(new NTable::TTableClient(*Driver, tableClientSettings));

    try {
        if (RunParams.Seed > 0) {
            Seed = RunParams.Seed;
        }
        Rng.ConstructInPlace(Seed);
        
        Log.Write(TLOG_INFO, "Seed: " + std::to_string(Seed));

        // Create constants
        TFastRng32 rng(Rng->GenRand(), 0);
        RunParams.CustomerIdDelta = UniformRandom32(0, 1023, rng);
        RunParams.OrderLineItemIdDelta = UniformRandom32(0, 8191, rng);
        
        // Thread resource

        ui32 terminalCount = RunParams.Warehouses * EWorkloadConstants::TPCC_TERMINAL_PER_WH;

        Scheduler = std::make_shared<TTaskScheduler>(RunParams.SchedulerThreads);
        ThreadPool = std::make_shared<TThreadPool>();
        
        Scheduler->Start();
        ThreadPool->Start(RunParams.Threads);
        
        Log.Write(TLOG_INFO, "Warm-up started (" + std::to_string(RunParams.WarmupDurationS) + "s)");

        ui64 warmupUs = RunParams.WarmupDurationS * 1'000'000ll;

        std::vector<ui64> terminalStartTime;
        for (ui32 i = 0; i < terminalCount; i++) {
            // we want to add all terminals in 80% of the warm-up time
            terminalStartTime.push_back(UniformRandom32(0, warmupUs * 4 / 5, rng));
        }
        std::sort(terminalStartTime.begin(), terminalStartTime.end());

        ui32 whId = RunParams.WarehouseStartId;
        ui32 whEnd = RunParams.Warehouses + RunParams.WarehouseStartId - 1;
        const ui32 termPerWh = EWorkloadConstants::TPCC_TERMINAL_PER_WH;
        ui32 terminalStartId = (whId - 1) * termPerWh + 1;
        ui32 terminalLocalId = 1;

        auto readyTerminals = std::make_shared<std::deque<TTerminal*>>();
        TMutex readyQueueMutex;

        TInstant warmupStart = Now();
        while (whId <= whEnd) {
            ui32 terminalId = (whId - 1) * termPerWh + terminalLocalId;

            i64 sleepUntilUs = warmupStart.MicroSeconds() + terminalStartTime[terminalId - terminalStartId];
            i64 nowUs = Now().MicroSeconds();
            
            if (sleepUntilUs > nowUs) {
                Sleep(TDuration::MicroSeconds(sleepUntilUs - nowUs));
            }

            Terminals.push_back(std::make_shared<TTerminal>(
                RunParams, whId, terminalId, Log, tableClient, Scheduler, ThreadPool, readyTerminals, readyQueueMutex, Rng->GenRand()
            ));

            Terminals.back()->Start();

            if (Terminals.size() % 1000 == 0) {
                Log.Write(TLOG_INFO, std::to_string(Terminals.size()) + " terminals started");
            }

            ++terminalLocalId;
            if (terminalLocalId > termPerWh) {
                ++whId;
                terminalLocalId = 1;
            }
        }

        // Sleep 20% of the warm-up time
        i64 warmupEndUs = warmupUs + warmupStart.MicroSeconds();
        i64 nowUs = Now().MicroSeconds();

        Sleep(TDuration::MicroSeconds(std::max(i64(0), warmupEndUs - nowUs)));

        Log.Write(TLOG_INFO, "Warm-up ended");

        Log.Write(TLOG_INFO, "Workload started (" + std::to_string(RunParams.DurationS) +  "s)");

        for (auto& terminal: Terminals) {
            terminal->NotifyAboutWorkloadStart();
        }

        i64 workloadEndUs = warmupEndUs + RunParams.DurationS * 1'000'000ll;
        nowUs = Now().MicroSeconds();

        Sleep(TDuration::MicroSeconds(std::max(i64(0), workloadEndUs - nowUs)));

        for (auto& terminal: Terminals) {
            terminal->NotifyAboutWorkloadEnd();
        }

        Log.Write(TLOG_INFO, "Workload ended");

        ThreadPool->Stop();
        Scheduler->Stop();

        Log.Write(TLOG_INFO, "Collecting results...");

        CollectAndPrintResult();

        tableClient->Stop();

    } catch (const std::exception& ex) {
        tableClient->Stop();

        TStringStream stream;
        stream<< "Run workload: "  << ex.what();
        Log.Write(TLOG_ERR, stream.Str());
        return EXIT_FAILURE;
    } catch (...) {
        tableClient->Stop();

        Log.Write(TLOG_ERR, "Failed with an unknown error"); 
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

}
}
