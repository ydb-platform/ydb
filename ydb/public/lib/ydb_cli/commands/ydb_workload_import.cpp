#include "ydb_workload_import.h"
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/threading/future/async.h>
#include <util/generic/deque.h>
#include <thread>

namespace NYdb::NConsoleClient {

TWorkloadCommandImport::TWorkloadCommandImport(NYdbWorkload::TWorkloadParams& workloadParams, NYdbWorkload::TWorkloadDataInitializer::TList initializers)
    : TClientCommandTree("import", {}, "Fill tables for workload with data.")
    , WorkloadParams(workloadParams)
{
    for (auto initializer: initializers) {
        AddCommand(std::make_unique<TUploadCommand>(workloadParams, UploadParams, initializer));
    }
}

void TWorkloadCommandImport::Config(TConfig& config) {
    TClientCommandTree::Config(config);
    config.Opts->AddLongOption('t', "upload-threads", "Number of threads to generate tables content.")
        .Optional().DefaultValue(UploadParams.Threads).StoreResult(&UploadParams.Threads);
    config.Opts->AddLongOption("bulk-size", "Data portion size in rows for upload.")
        .DefaultValue(WorkloadParams.BulkSize).StoreResult(&WorkloadParams.BulkSize);
    config.Opts->AddLongOption("max-in-flight", "Maximum number if data portions that can be simultaneously in process.")
        .DefaultValue(UploadParams.MaxInFlight).StoreResult(&UploadParams.MaxInFlight);
}

TWorkloadCommandImport::TUploadParams::TUploadParams()
    : Threads(std::thread::hardware_concurrency())
{}

void TWorkloadCommandImport::TUploadCommand::Config(TConfig& config) {
    TWorkloadCommandBase::Config(config);
    Initializer->ConfigureOpts(*config.Opts);
}

TWorkloadCommandImport::TUploadCommand::TUploadCommand(NYdbWorkload::TWorkloadParams& workloadParams, const TUploadParams& uploadParams, NYdbWorkload::TWorkloadDataInitializer::TPtr initializer)
    : TWorkloadCommandBase(initializer->GetName(), workloadParams, NYdbWorkload::TWorkloadParams::ECommandType::Import, initializer->GetDescription())
    , UploadParams(uploadParams)
    , Initializer(initializer)
{}

int TWorkloadCommandImport::TUploadCommand::DoRun(NYdbWorkload::IWorkloadQueryGenerator& /*workloadGen*/, TConfig& /*config*/) {
    auto dataGeneratorList = Initializer->GetBulkInitialData();
    AtomicSet(ErrorsCount, 0);
    InFlightSemaphore = NThreading::TAsyncSemaphore::Make(UploadParams.MaxInFlight);
    for (auto dataGen : dataGeneratorList) {
        TThreadPoolParams params;
        params.SetCatching(false);
        TThreadPool pool;
        pool.Start(UploadParams.Threads);
        const auto start = Now();
        Cout << "Fill table " << dataGen->GetName() << "..."  << Endl;
        Bar = MakeHolder<TProgressBar>(dataGen->GetSize());
        TVector<NThreading::TFuture<void>> sendings;
        for (ui32 t = 0; t < UploadParams.Threads; ++t) {
            sendings.push_back(NThreading::Async([this, dataGen] () {
                ProcessDataGenerator(dataGen);
            }, pool));
        }
        NThreading::WaitAll(sendings).Wait();
        const bool wereErrors = AtomicGet(ErrorsCount);
        Cout << "Fill table " << dataGen->GetName() << (wereErrors ? "Failed" : "OK" ) << " " << Bar->GetCurProgress() << " / " << Bar->GetCapacity() << " (" << (Now() - start) << ")" << Endl;
        if (wereErrors) {
            break;
        }
    }
    return AtomicGet(ErrorsCount) ? EXIT_FAILURE : EXIT_SUCCESS;
}

TAsyncStatus TWorkloadCommandImport::TUploadCommand::SendDataPortion(NYdbWorkload::IBulkDataGenerator::TDataPortionPtr portion) const {
    auto convertResult = [](const NTable::TAsyncBulkUpsertResult& result) {
            return TStatus(result.GetValueSync());
        };
    if (auto* value = std::get_if<TValue>(&portion->MutableData())) {
        return TableClient->BulkUpsert(portion->GetTable(), std::move(*value)).Apply(convertResult);
    }
    NRetry::TRetryOperationSettings retrySettings;
    retrySettings.RetryUndefined(true);
    retrySettings.MaxRetries(30);
    if (auto* value = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TCsv>(&portion->MutableData())) {
        return TableClient->RetryOperation([value, portion, convertResult](NTable::TTableClient& client) {
            NTable::TBulkUpsertSettings settings;
            settings.FormatSettings(value->FormatString);
            return client.BulkUpsert(portion->GetTable(), NTable::EDataFormat::CSV, value->Data, TString(), settings)
                .Apply(convertResult);
        }, retrySettings);
    }
    if (auto* value = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TArrow>(&portion->MutableData())) {
        return TableClient->RetryOperation([value, portion, convertResult](NTable::TTableClient& client) {
            return client.BulkUpsert(portion->GetTable(), NTable::EDataFormat::ApacheArrow, value->Data, value->Schema)
                .Apply(convertResult);
        }, retrySettings);
    }
    Y_FAIL_S("Invalid data portion");
}

void TWorkloadCommandImport::TUploadCommand::ProcessDataGenerator(std::shared_ptr<NYdbWorkload::IBulkDataGenerator> dataGen) noexcept try {
    TDeque<NThreading::TFuture<void>> sendings;
    for (auto portions = dataGen->GenerateDataPortion(); !portions.empty() && !AtomicGet(ErrorsCount); portions = dataGen->GenerateDataPortion()) {
        for (const auto& data: portions) {
            sendings.emplace_back(
                InFlightSemaphore->AcquireAsync().Apply([this, data](const auto& sem) {
                    auto ar = MakeAtomicShared<NThreading::TAsyncSemaphore::TAutoRelease>(sem.GetValueSync()->MakeAutoRelease());
                    return SendDataPortion(data).Apply(
                        [ar, data, this](const TAsyncStatus& result) {
                            const auto& res = result.GetValueSync();
                            data->SetSendResult(res);
                            auto guard = Guard(Lock);
                            if (!res.IsSuccess()) {
                                Cerr << "Bulk upset to " << data->GetTable() << " failed, " << res.GetStatus() << ", " << res.GetIssues().ToString() << Endl;
                                AtomicIncrement(ErrorsCount);
                            } else {
                                Bar->AddProgress(data->GetSize());
                            }
                        });
                    }
                )
            );
            while(sendings.size() > UploadParams.MaxInFlight) {
                sendings.pop_front();
            }
        }
        if (AtomicGet(ErrorsCount)) {
            break;
        }
    }
    NThreading::WaitAll(sendings).GetValueSync();
} catch (...) {
    auto g = Guard(Lock);
    Cerr << "Fill table " << dataGen->GetName() << " failed: " << CurrentExceptionMessage() << ", backtrace: ";
    PrintBackTrace();
    AtomicSet(ErrorsCount, 1);
}
}