#include "ydb_workload.h"

#include "click_bench.h"
#include "tpch.h"
#include "topic_workload/topic_workload.h"
#include "transfer_workload/transfer_workload.h"
#include "query_workload.h"
#include "ydb/library/yverify_stream/yverify_stream.h"

#include <ydb/library/workload/workload_factory.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <library/cpp/threading/local_executor/local_executor.h>

#include <util/random/random.h>
#include <util/system/spinlock.h>
#include <util/thread/pool.h>

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
    AddCommand(std::make_unique<TCommandClickBench>());
    AddCommand(std::make_unique<TCommandWorkloadTopic>());
    AddCommand(std::make_unique<TCommandWorkloadTransfer>());
    AddCommand(std::make_unique<TCommandTpch>());
    AddCommand(std::make_unique<TCommandQueryWorkload>());
    for (const auto& key: NYdbWorkload::TWorkloadFactory::GetRegisteredKeys()) {
        AddCommand(std::make_unique<TWorkloadCommandRoot>(key.c_str()));
    }
}

TWorkloadCommand::TWorkloadCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    : TYdbCommand(name, aliases, description)
    , TotalSec(0)
    , Threads(0)
    , Rate(0)
    , ClientTimeoutMs(0)
    , OperationTimeoutMs(0)
    , CancelAfterTimeoutMs(0)
    , WindowSec(0)
    , Quiet(false)
    , Verbose(false)
    , PrintTimestamp(false)
    , QueryExecuterType()
    , WindowHist(60000, 2) // highestTrackableValue 60000ms = 60s, precision 2
    , TotalHist(60000, 2)
    , TotalQueries(0)
    , TotalRetries(0)
    , WindowRetryCount(0)
    , TotalErrors(0)
    , WindowErrors(0)
{}

void TWorkloadCommand::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption('s', "seconds", "Seconds to run workload.")
        .DefaultValue(10).StoreResult(&TotalSec);
    config.Opts->AddLongOption('t', "threads", "Number of parallel threads in workload.")
        .DefaultValue(10).StoreResult(&Threads);

    const auto name = Parent->Parent->Name;
    if (name == "kv") {
        config.Opts->AddLongOption("rate", "Total rate for all threads (requests per second).")
            .DefaultValue(0).StoreResult(&Rate);
    }
    else if (name == "stock") {
        config.Opts->AddLongOption("rate", "Total rate for all threads (transactions per second).")
            .DefaultValue(0).StoreResult(&Rate);
    }

    config.Opts->AddLongOption("quiet", "Quiet mode. Doesn't print statistics each second.")
        .StoreTrue(&Quiet);
    config.Opts->AddLongOption("print-timestamp", "Print timestamp each second with statistics.")
        .StoreTrue(&PrintTimestamp);
    config.Opts->AddLongOption("client-timeout", "Client timeout in ms.")
        .DefaultValue(1000).StoreResult(&ClientTimeoutMs);
    config.Opts->AddLongOption("operation-timeout", "Operation timeout in ms.")
        .DefaultValue(800).StoreResult(&OperationTimeoutMs);
    config.Opts->AddLongOption("cancel-after", "Cancel after timeout in ms.")
        .DefaultValue(800).StoreResult(&CancelAfterTimeoutMs);
    config.Opts->AddLongOption("window", "Window duration in seconds.")
        .DefaultValue(1).StoreResult(&WindowSec);
    config.Opts->AddLongOption("executer", "Query executer type (data or generic).")
        .DefaultValue("data").StoreResult(&QueryExecuterType);
}

void TWorkloadCommand::PrepareForRun(TConfig& config) {
    SetRandomSeed(Now().MicroSeconds());

    auto driverConfig = TDriverConfig()
        .SetEndpoint(config.Address)
        .SetDatabase(config.Database)
        .SetBalancingPolicy(EBalancingPolicy::UseAllNodes)
        .SetCredentialsProviderFactory(config.CredentialsGetter(config));

    Verbose = config.IsVerbose();
    if (config.EnableSsl) {
        driverConfig.UseSecureConnection(config.CaCerts);
    }
    Driver = std::make_unique<NYdb::TDriver>(NYdb::TDriver(driverConfig));
    if (QueryExecuterType == "data") {
        auto tableClientSettings = NTable::TClientSettings()
                            .SessionPoolSettings(
                                NTable::TSessionPoolSettings()
                                    .MaxActiveSessions(10+Threads));
        TableClient = std::make_unique<NTable::TTableClient>(*Driver, tableClientSettings);
    } else if (QueryExecuterType == "generic") {
        auto queryClientSettings = NQuery::TClientSettings()
                            .SessionPoolSettings(
                                NQuery::TSessionPoolSettings()
                                    .MaxActiveSessions(10+Threads));
        QueryClient = std::make_unique<NQuery::TQueryClient>(*Driver, queryClientSettings);
    } else {
        Y_FAIL_S("Unexpected executor Type: " + QueryExecuterType);
    }
}

void TWorkloadCommand::WorkerFn(int taskId, NYdbWorkload::IWorkloadQueryGenerator& workloadGen, const int type) {
    const auto dataQuerySettings = NYdb::NTable::TExecDataQuerySettings()
            .KeepInQueryCache(true)
            .OperationTimeout(TDuration::MilliSeconds(OperationTimeoutMs))
            .ClientTimeout(TDuration::MilliSeconds(ClientTimeoutMs))
            .CancelAfter(TDuration::MilliSeconds(CancelAfterTimeoutMs));
    const auto genericQuerySettings = NYdb::NQuery::TExecuteQuerySettings()
            .ClientTimeout(TDuration::MilliSeconds(ClientTimeoutMs));
    int retryCount = -1;
    NYdbWorkload::TQueryInfo queryInfo;

    auto runTableClient = [this, &queryInfo, &dataQuerySettings, &retryCount] (NYdb::NTable::TSession session) -> NYdb::TStatus {
        if (!TableClient) {
            Y_FAIL_S("Only data query executer type supported.");
        }
        ++retryCount;
        if (queryInfo.AlterTable) {
            auto result = TableClient->RetryOperationSync([&queryInfo](NTable::TSession session) {
                return session.AlterTable(queryInfo.TablePath, queryInfo.AlterTable.value()).GetValueSync();
            });
            return result;
        } else if (queryInfo.UseReadRows) {
            auto result = TableClient->ReadRows(queryInfo.TablePath, std::move(*queryInfo.KeyToRead))
                .GetValueSync();
            if (queryInfo.ReadRowsResultCallback) {
                queryInfo.ReadRowsResultCallback.value()(result);
            }
            return result;
        } else {
            auto result = session.ExecuteDataQuery(queryInfo.Query.c_str(),
                NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(),
                queryInfo.Params, dataQuerySettings
            ).GetValueSync();
            if (queryInfo.DataQueryResultCallback) {
                queryInfo.DataQueryResultCallback.value()(result);
            }
            return result;
        }
    };

    auto runQueryClient = [this, &queryInfo, &genericQuerySettings, &retryCount] (NYdb::NQuery::TSession session) -> NYdb::NQuery::TAsyncExecuteQueryResult {
        if (!QueryClient) {
            Y_FAIL_S("Only generic query executer type supported.");
        }
        ++retryCount;
        if (queryInfo.AlterTable) {
            Y_FAIL_S("Generic query doesn't support alter table.");
        } else if (queryInfo.UseReadRows) {
            Y_FAIL_S("Generic query doesn't support readrows.");
        } else {
            auto result = session.ExecuteQuery(queryInfo.Query.c_str(),
                NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx(),
                queryInfo.Params, genericQuerySettings
            );
            return result;
        }
    };

    auto runQuery = [this, &runQueryClient, &runTableClient, &queryInfo]() -> NYdb::TStatus {
        Y_ENSURE_BT(TableClient || QueryClient);
        if (TableClient) {
            return TableClient->RetryOperationSync(runTableClient);
        } else {
            auto result = QueryClient->RetryQuery(runQueryClient).GetValueSync();
            if (queryInfo.GenericQueryResultCallback) {
                queryInfo.GenericQueryResultCallback.value()(result);
            }
            return result;
        }
    };

    while (Now() < StopTime) {
        auto queryInfoList = workloadGen.GetWorkload(type);
        if (queryInfoList.empty()) {
            Cerr << "Task ID: " << taskId << ". No queries to run." << Endl;
            return;
        }

        auto opStartTime = Now();
        NYdbWorkload::TQueryInfoList::iterator it;
        for (it = queryInfoList.begin(); it != queryInfoList.end(); ) {

            if (Rate != 0)
            {
                const ui64 expectedQueries = (Now() - StartTime).SecondsFloat() * Rate;
                if (TotalQueries > expectedQueries) {
                    Sleep(TDuration::MilliSeconds(1));
                    continue;
                }
            }

            queryInfo = *it;
            auto status = runQuery();
            if (status.IsSuccess()) {
                TotalQueries++;
            } else {
                TotalErrors++;
                WindowErrors++;
                if (Verbose && status.GetStatus() != EStatus::ABORTED) {
                    Cerr << "Task ID: " << taskId << " Status: " << status.GetStatus() << " " << status.GetIssues().ToString() << Endl;
                }
                break;
            }
            if (retryCount > 0) {
                TotalRetries += retryCount;
                WindowRetryCount += retryCount;
            }
            retryCount = -1;

            ++it;
        }
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

int TWorkloadCommand::RunWorkload(NYdbWorkload::IWorkloadQueryGenerator& workloadGen, const int type) {
    if (!Quiet) {
        std::cout << "Window\tTxs/Sec\tRetries\tErrors\tp50(ms)\tp95(ms)\tp99(ms)\tpMax(ms)";
        if (PrintTimestamp) {
            std::cout << "\tTimestamp";
        }
        std::cout << std::endl;
    }

    StartTime = Now();
    StopTime = StartTime + TDuration::Seconds(TotalSec);

    NPar::LocalExecutor().RunAdditionalThreads(Threads);
    auto futures = NPar::LocalExecutor().ExecRangeWithFutures([this, &workloadGen, type](int id) {
        try {
            WorkerFn(id, workloadGen, type);
        } catch (std::exception& error) {
            Y_FAIL_S(error.what());
        }
    }, 0, Threads, NPar::TLocalExecutor::MED_PRIORITY);

    int windowIt = 1;
    auto windowDuration = TDuration::Seconds(WindowSec);
    while (Now() < StopTime) {
        if (StartTime + windowIt * windowDuration < Now()) {
            PrintWindowStats(windowIt++);
        }
        Sleep(std::max(TDuration::Zero(), Now() - StartTime - windowIt * windowDuration));
    }

    for (auto f : futures) {
        f.Wait();
    }

    PrintWindowStats(windowIt++);

    auto stats = GetWorkloadStats(TotalHist);
    std::cout << std::endl << "Txs\tTxs/Sec\tRetries\tErrors\tp50(ms)\tp95(ms)\tp99(ms)\tpMax(ms)" << std::endl
        << stats.OpsCount << "\t" << std::setw(7) << stats.OpsCount / (TotalSec * 1.0) << "\t" << TotalRetries.load() << "\t"
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
        std::cout << windowIt << "\t" << std::setw(7) << stats.OpsCount / WindowSec << "\t" << retries << "\t"
            << errors << "\t" << stats.Percentile50 << "\t" << stats.Percentile95 << "\t"
            << stats.Percentile99 << "\t" << stats.Percentile100;
        if (PrintTimestamp) {
            std::cout << "\t" << Now().ToStringUpToSeconds();
        }
        std::cout << std::endl;
    }
}

namespace {
    bool WaitBulk(const NYdb::NTable::TAsyncBulkUpsertResult& prevResult, TAtomic& errors, const std::string& table, TAdaptiveLock& lock) {
        if (prevResult.Initialized()) {
            try {
                const auto& res = prevResult.GetValueSync();
                if (!res.IsSuccess()) {
                    auto g = Guard(lock);
                    Cerr << "Bulk upset to " << table << " failed: " << res.GetIssues().ToString() << Endl;
                    AtomicIncrement(errors);
                    return false;
                }
            } catch (...) {
                auto g = Guard(lock);
                Cerr << "Bulk upset to " << table << " failed: " << CurrentExceptionMessage() << ", backtrace: ";
                PrintBackTrace();
                AtomicIncrement(errors);
                return false;
            }
        }
        return true;
    }
}

TWorkloadCommandInit::TWorkloadCommandInit(const TString& key)
    : TWorkloadCommandBase("init", key, NYdbWorkload::TWorkloadParams::ECommandType::Init, "Create and initialize tables for workload")
{}

int TWorkloadCommandInit::Run(TConfig& config) {
    Driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    TableClient = std::make_unique<NTable::TTableClient>(*Driver);
    Params->DbPath = config.Database;
    auto workloadGen = Params->CreateGenerator();
    return InitTables(*workloadGen);
}

void TWorkloadCommandInit::Config(TConfig& config) {
    TYdbCommand::Config(config);
    Params->ConfigureOpts(*config.Opts, CommandType, Type);
}

TWorkloadCommandRun::TWorkloadCommandRun(const TString& key, const NYdbWorkload::IWorkloadQueryGenerator::TWorkloadType& workload)
    : TWorkloadCommandBase(workload.CommandName, key, NYdbWorkload::TWorkloadParams::ECommandType::Run, workload.Description)
{
    Type = workload.Type;
}

int TWorkloadCommandRun::Run(TConfig& config) {
    PrepareForRun(config);
    Params->DbPath = config.Database;
    auto workloadGen = Params->CreateGenerator();
    return RunWorkload(*workloadGen, Type);
}


TWorkloadCommandClean::TWorkloadCommandClean(const TString& key)
    : TWorkloadCommandBase("clean", key, NYdbWorkload::TWorkloadParams::ECommandType::Clean, "Drop tables created in init phase")
{}

int TWorkloadCommandClean::Run(TConfig& config) {
    Params->DbPath = config.Database;
    auto workloadGen = Params->CreateGenerator();
    return CleanTables(*workloadGen, config);
}


TWorkloadCommandBase::TWorkloadCommandBase(const TString& name, const TString& key, const NYdbWorkload::TWorkloadParams::ECommandType commandType, const TString& description /*= TString()*/)
    : TWorkloadCommand(name, std::initializer_list<TString>(), description)
    , CommandType(commandType)
    , Params(NYdbWorkload::TWorkloadFactory::MakeHolder(key))
{}

void TWorkloadCommandBase::Config(TConfig& config) {
    TWorkloadCommand::Config(config);
    Params->ConfigureOpts(*config.Opts, CommandType, Type);
}


TWorkloadCommandRoot::TWorkloadCommandRoot(const TString& key)
    : TClientCommandTree(key, {}, "YDB " + NYdbWorkload::TWorkloadFactory::MakeHolder(key)->GetWorkloadName() + " workload")
{
    AddCommand(std::make_unique<TWorkloadCommandInit>(key));
    auto supportedWorkloads = NYdbWorkload::TWorkloadFactory::MakeHolder(key)->CreateGenerator()->GetSupportedWorkloadTypes();
    switch (supportedWorkloads.size()) {
    case 0:
        break;
    case 1:
        supportedWorkloads.back().CommandName = "run";
        AddCommand(std::make_unique<TWorkloadCommandRun>(key, supportedWorkloads.back()));
        break;
    default: {
        auto run = std::make_unique<TClientCommandTree>("run", std::initializer_list<TString>(), "Run YDB " + NYdbWorkload::TWorkloadFactory::MakeHolder(key)->GetWorkloadName() + " workload");
        for (const auto& type: supportedWorkloads) {
            run->AddCommand(std::make_unique<TWorkloadCommandRun>(key, type));
        }
        AddCommand(std::move(run));
        break;
    }
    }
    AddCommand(std::make_unique<TWorkloadCommandClean>(key));
}

int TWorkloadCommand::InitTables(NYdbWorkload::IWorkloadQueryGenerator& workloadGen) {
    auto session = GetSession();
    auto result = session.ExecuteSchemeQuery(workloadGen.GetDDLQueries()).GetValueSync();
    ThrowOnError(result);

    auto queryInfoList = workloadGen.GetInitialData();
    for (auto queryInfo : queryInfoList) {
        auto prepareResult = session.PrepareDataQuery(queryInfo.Query.c_str()).GetValueSync();
        if (!prepareResult.IsSuccess()) {
            Cerr << "Prepare failed: " << prepareResult.GetIssues().ToString() << Endl
                << "Query:\n" << queryInfo.Query << Endl;
            return EXIT_FAILURE;
        }

        auto dataQuery = prepareResult.GetQuery();
        auto result = dataQuery.Execute(NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(),
                                        std::move(queryInfo.Params)).GetValueSync();
        if (!result.IsSuccess()) {
            Cerr << "Query execution failed: " << result.GetIssues().ToString() << Endl
                << "Query:\n" << queryInfo.Query << Endl;
            return EXIT_FAILURE;
        }
    }

    auto dataGeneratorList = workloadGen.GetBulkInitialData();
    TAdaptiveThreadPool pool;
    pool.Start();
    TAtomic errors = 0;
    TAdaptiveLock lock;
    for (auto dataGen : dataGeneratorList) {
        pool.SafeAddFunc([dataGen, this, &errors, &lock] {
            NYdb::NTable::TAsyncBulkUpsertResult prevResult;
            for (auto data = dataGen->GenerateDataPortion(); data.Defined() && !AtomicGet(errors); data = dataGen->GenerateDataPortion()) {
                if (WaitBulk(prevResult, errors, dataGen->GetTable(), lock)) {
                    prevResult = TableClient->BulkUpsert(dataGen->GetTable(), std::move(*data));
                }
            }
            if (WaitBulk(prevResult, errors, dataGen->GetTable(), lock)) {
                auto g = Guard(lock);
                Cout << "Fill table " << dataGen->GetTable() << "..."  << (AtomicGet(errors) ? "Breaked" : "OK" ) << Endl;
            }
        });
    }
    pool.Stop();
    return AtomicGet(errors) ? EXIT_FAILURE : EXIT_SUCCESS;
}

int TWorkloadCommand::CleanTables(const NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& config) {
    auto driver = CreateDriver(config);
    NTable::TTableClient tableClient(driver);
    NTopic::TTopicClient topicClient(driver);
    NScheme::TSchemeClient schemeClient(driver);
    auto pathsToDelete = workloadGen.GetCleanPaths();
    NScheme::TRemoveDirectorySettings settings;
    for (const auto& path : pathsToDelete) {
        auto fullPath = config.Database + "/" + path.c_str();
        ThrowOnError(RemovePathRecursive(schemeClient, tableClient, topicClient, fullPath, ERecursiveRemovePrompt::Never, settings));
    }

    return EXIT_SUCCESS;
}

NTable::TSession TWorkloadCommand::GetSession() {
    NTable::TCreateSessionResult result = TableClient->GetSession(NTable::TCreateSessionSettings()).GetValueSync();
    ThrowOnError(result);
    return result.GetSession();
}

} // namespace NYdb::NConsoleClient
