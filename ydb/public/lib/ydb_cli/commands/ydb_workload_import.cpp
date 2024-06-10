#include "ydb_workload_import.h"
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/threading/future/async.h>

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
    config.Opts->AddLongOption('t', "upload-threads", "Number of threads to generate and upload tables content.")
        .Optional().DefaultValue(UploadParams.Threads).StoreResult(&UploadParams.Threads);
    config.Opts->AddLongOption("bulk-size", "Data portion size in rows for upload.")
        .DefaultValue(WorkloadParams.BulkSize).StoreResult(&WorkloadParams.BulkSize);
}

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
    TAtomic stop = 0;
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
            sendings.push_back(NThreading::Async([this, dataGen, &stop] () {
                if (!ProcessDataGenerator(dataGen, stop)) {
                    AtomicSet(stop, 1);
                }
            }, pool));
        }
        NThreading::WaitAll(sendings).Wait();
        const bool wasErrors = AtomicGet(stop);
        Cout << "Fill table " << dataGen->GetName() << "..."  << (wasErrors ? "Breaked" : "OK" ) << " " << Bar->GetCurProgress() << " / " << Bar->GetCapacity() << " (" << (Now() - start) << ")" << Endl;
        if (wasErrors) {
            break;
        }
    }
    return AtomicGet(stop) ? EXIT_FAILURE : EXIT_SUCCESS;
}

TStatus TWorkloadCommandImport::TUploadCommand::SendDataPortion(NYdbWorkload::IBulkDataGenerator::TDataPortionPtr portion) const {
    if (auto* value = std::get_if<TValue>(&portion->Data)) {
        return TableClient->BulkUpsert(portion->Table, std::move(*value)).GetValueSync();
    }
    NRetry::TRetryOperationSettings retrySettings;
    retrySettings.RetryUndefined(true);
    retrySettings.MaxRetries(10000);
    if (auto* value = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TCsv>(&portion->Data)) {
        return TableClient->RetryOperationSync([value, portion](NTable::TTableClient& client) {
            NTable::TBulkUpsertSettings settings;
            settings.FormatSettings(value->FormatString);
            return client.BulkUpsert(portion->Table, NTable::EDataFormat::CSV, value->Data, TString(), settings).GetValueSync();
        }, retrySettings);
    }
    if (auto* value = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TArrow>(&portion->Data)) {
        return TableClient->RetryOperationSync([value, portion](NTable::TTableClient& client) {
            return client.BulkUpsert(portion->Table, NTable::EDataFormat::ApacheArrow, value->Data, value->Schema).GetValueSync();
        }, retrySettings);
    }
    Y_FAIL_S("Invalid data portion");
}

bool TWorkloadCommandImport::TUploadCommand::ProcessDataGenerator(std::shared_ptr<NYdbWorkload::IBulkDataGenerator> dataGen, const TAtomic& stop) noexcept try {
    for (auto portions = dataGen->GenerateDataPortion(); !portions.empty() && !AtomicGet(stop); portions = dataGen->GenerateDataPortion()) {
        for (const auto& data: portions) {
            const auto res = SendDataPortion(data);
            auto g = Guard(Lock);
            if (!res.IsSuccess()) {
                Cerr << "Bulk upset to " << dataGen->GetName() << " failed, " << res.GetStatus() << ", " << res.GetIssues().ToString() << Endl;
                return false;
            }
            Bar->AddProgress(data->Size);
        }
    }
    return true;
} catch (...) {
    auto g = Guard(Lock);
    Cerr << "Fill table " << dataGen->GetName() << " failed: " << CurrentExceptionMessage() << ", backtrace: ";
    PrintBackTrace();
    return false;
}
}