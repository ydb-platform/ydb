#include "ydb_workload.h" 
 
#include "stock_workload.h" 
 
#include <ydb/library/workload/workload_factory.h> 
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h> 
 
#include <library/cpp/threading/local_executor/local_executor.h> 
 
#include <atomic> 
#include <iomanip> 
 
namespace NYdb::NConsoleClient { 
 
struct TWorkloadStats { 
    ui64 OpsCount; 
    ui64 Percentile50; 
    ui64 Percentile95; 
    ui64 Percentile99; 
    ui64 Percentile100; 
}; 
 
TWorkloadStats GetWorkloadStats(const NHdr::THistogram& hdr) { 
    TWorkloadStats stats; 
    stats.OpsCount = hdr.GetTotalCount(); 
    stats.Percentile50 = hdr.GetValueAtPercentile(50.0); 
    stats.Percentile95 = hdr.GetValueAtPercentile(95.0); 
    stats.Percentile99 = hdr.GetValueAtPercentile(99.0); 
    stats.Percentile100 = hdr.GetMax(); 
    return stats; 
} 
 
TCommandWorkload::TCommandWorkload() 
    : TClientCommandTree("workload", {}, "YDB workload service") 
{ 
    AddCommand(std::make_unique<TCommandStock>()); 
} 
 
TWorkloadCommand::TWorkloadCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description) 
    : TYdbCommand(name, aliases, description) 
    , Seconds(0) 
    , Threads(0) 
    , Quiet(false) 
    , PrintTimestamp(false) 
    , WindowHist(1000, 2) // highestTrackableValue 1000ms = 1s, precision 2 
    , TotalHist(1000, 2) 
    , TotalRetries(0) 
    , WindowRetryCount(0) 
    , TotalErrors(0) 
    , WindowErrors(0) 
{} 
 
void TWorkloadCommand::Config(TConfig& config) { 
    TYdbCommand::Config(config); 
 
    config.Opts->AddLongOption('s', "seconds", "Seconds to run workload.") 
        .DefaultValue(10).StoreResult(&Seconds); 
    config.Opts->AddLongOption('t', "threads", "Number of parallel threads in workload.") 
        .DefaultValue(10).StoreResult(&Threads); 
    config.Opts->AddLongOption("quiet", "Quiet mode. Doesn't print statistics each second.") 
        .StoreTrue(&Quiet); 
    config.Opts->AddLongOption("print-timestamp", "Print timestamp each second with statistics.") 
        .StoreTrue(&PrintTimestamp); 
} 
 
void TWorkloadCommand::PrepareForRun(TConfig& config) { 
    auto driverConfig = TDriverConfig() 
        .SetEndpoint(config.Address) 
        .SetDatabase(config.Database) 
        .SetBalancingPolicy(EBalancingPolicy::UseAllNodes) 
        .SetCredentialsProviderFactory(config.CredentialsGetter(config)); 
     
    if (config.EnableSsl) { 
        driverConfig.UseSecureConnection(config.CaCerts); 
    } 
    Driver = std::make_unique<NYdb::TDriver>(NYdb::TDriver(driverConfig)); 
    auto tableClientSettings = NTable::TClientSettings() 
                        .SessionPoolSettings( 
                            NTable::TSessionPoolSettings() 
                                .MaxActiveSessions(10+Threads)); 
    TableClient = std::make_unique<NTable::TTableClient>(*Driver, tableClientSettings); 
} 
 
void TWorkloadCommand::WorkerFn(int taskId, TWorkloadQueryGenPtr workloadGen, const int type) { 
    auto querySettings = NYdb::NTable::TExecDataQuerySettings() 
            .KeepInQueryCache(true) 
            .ClientTimeout(TDuration::Seconds(2)); 
    int retryCount = -1; 
 
    NYdbWorkload::TQueryInfo queryInfo; 
    auto runQuery = [&queryInfo, &querySettings, &retryCount] (NYdb::NTable::TSession session) -> NYdb::TStatus { 
        ++retryCount; 
        TStatus result(EStatus::SUCCESS, NYql::TIssues()); 
        result = session.ExecuteDataQuery(queryInfo.Query.c_str(), 
            NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(), 
            queryInfo.Params, querySettings 
        ).GetValueSync(); 
        return result; 
    }; 
 
    while (Now() < StopTime) { 
        auto queryInfoList = workloadGen->GetWorkload(type); 
        if (queryInfoList.empty()) { 
            Cerr << "Task ID: " << taskId << ". No queries to run." << Endl; 
            return; 
        } 
 
        auto opStartTime = Now(); 
        NYdbWorkload::TQueryInfoList::iterator it; 
        for (it = queryInfoList.begin(); it != queryInfoList.end(); ++it) { 
            queryInfo = *it; 
            auto status = TableClient->RetryOperationSync(runQuery); 
            if (!status.IsSuccess()) { 
                TotalErrors++; 
                WindowErrors++; 
                // if (status.GetStatus() != EStatus::ABORTED) { 
                    // Cerr << "Task ID: " << taskId << " Status: " << status.GetStatus() << " " << status.GetIssues().ToString() << Endl; 
                // } 
                break; 
            } 
        } 
        if (retryCount > 0) { 
            TotalRetries += retryCount; 
            WindowRetryCount += retryCount; 
        } 
        retryCount = -1; 
        if (it != queryInfoList.end()) { 
            continue; 
        } 
 
        ui64 latency = (Now() - opStartTime).MilliSeconds(); 
        with_lock(HdrLock) { 
            WindowHist.RecordValue(latency); 
            TotalHist.RecordValue(latency); 
        } 
    } 
    TotalRetries += std::max(retryCount, 0); 
    WindowRetryCount += std::max(retryCount, 0); 
} 
 
int TWorkloadCommand::RunWorkload(TWorkloadQueryGenPtr workloadGen, const int type) { 
    if (!Quiet) { 
        std::cout << "Elapsed\tTxs/Sec\tRetries\tErrors\tp50(ms)\tp95(ms)\tp99(ms)\tpMax(ms)"; 
        if (PrintTimestamp) { 
            std::cout << "\tTimestamp"; 
        } 
        std::cout << std::endl; 
    } 
 
    StartTime = Now(); 
    StopTime = StartTime + TDuration::Seconds(Seconds); 
 
    NPar::LocalExecutor().RunAdditionalThreads(Threads); 
    auto futures = NPar::LocalExecutor().ExecRangeWithFutures([this, &workloadGen, type](int id) { 
        WorkerFn(id, workloadGen, type); 
    }, 0, Threads, NPar::TLocalExecutor::MED_PRIORITY); 
 
    int windowIt = 1; 
    while (Now() < StopTime) { 
        if (StartTime + windowIt * WINDOW_DURATION < Now()) { 
            PrintWindowStats(windowIt++); 
        } 
        Sleep(std::max(TDuration::Zero(), Now() - StartTime - windowIt * WINDOW_DURATION)); 
    } 
 
    for (auto f : futures) { 
        f.Wait(); 
    } 
 
    PrintWindowStats(windowIt++); 
 
    auto stats = GetWorkloadStats(TotalHist); 
    std::cout << std::endl << "Txs\tTxs/Sec\tRetries\tErrors\tp50(ms)\tp95(ms)\tp99(ms)\tpMax(ms)" << std::endl 
        << stats.OpsCount << "\t" << std::setw(7) << stats.OpsCount / (Seconds * 1.0) << "\t" << TotalRetries.load() << "\t" 
        << TotalErrors.load() << "\t" << stats.Percentile50 << "\t" << stats.Percentile95 << "\t" 
        << stats.Percentile99 << "\t" << stats.Percentile100 << std::endl; 
 
    return EXIT_SUCCESS; 
} 
 
void TWorkloadCommand::PrintWindowStats(int windowIt) { 
    TWorkloadStats stats; 
    auto retries = WindowRetryCount.exchange(0); 
    auto errors = WindowErrors.exchange(0); 
    with_lock(HdrLock) { 
        stats = GetWorkloadStats(WindowHist); 
        WindowHist.Reset(); 
    } 
    if (!Quiet) { 
        std::cout << windowIt << "\t" << std::setw(7) << stats.OpsCount / WINDOW_DURATION.Seconds() << "\t" << retries << "\t" 
            << errors << "\t" << stats.Percentile50 << "\t" << stats.Percentile95 << "\t" 
            << stats.Percentile99 << "\t" << stats.Percentile100; 
        if (PrintTimestamp) { 
            std::cout << "\t" << Now().ToStringUpToSeconds(); 
        } 
        std::cout << std::endl; 
    } 
} 
 
} // namespace NYdb::NConsoleClient 
