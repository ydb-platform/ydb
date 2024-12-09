#include "ydb_workload_import.h"
#include <ydb/core/io_formats/arrow/table/table.h>
#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/import/cli_arrow_helpers.h>
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
    config.Opts->AddLongOption('f', "file-output-path", "Path to a directory to save tables into as files instead of uploading it to db.")
        .StoreResult(&UploadParams.FileOutputPath);
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

int TWorkloadCommandImport::TUploadCommand::DoRun(NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& config) {
    auto dataGeneratorList = Initializer->GetBulkInitialData();
    AtomicSet(ErrorsCount, 0);
    InFlightSemaphore = MakeHolder<TFastSemaphore>(UploadParams.MaxInFlight);
    if (UploadParams.FileOutputPath.IsDefined()) {
        Writer = MakeHolder<TFileWriter>(*this);
    } else {
        Writer = MakeHolder<TDbWriter>(*this, workloadGen, config);
    }
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
        Cout << "Fill table " << dataGen->GetName()  << " "<< (wereErrors ? "Failed" : "OK" ) << " " << Bar->GetCurProgress() << " / " << Bar->GetCapacity() << " (" << (Now() - start) << ")" << Endl;
        if (wereErrors) {
            break;
        }
    }
    return AtomicGet(ErrorsCount) ? EXIT_FAILURE : EXIT_SUCCESS;
}
class TWorkloadCommandImport::TUploadCommand::TDbWriter: public IWriter {
public:
    TDbWriter(TWorkloadCommandImport::TUploadCommand& owner, NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& config)
        : IWriter(owner)
    {
        RetrySettings.RetryUndefined(true);
        RetrySettings.MaxRetries(30);
        for(const auto& path: workloadGen.GetCleanPaths()) {
            const auto list = NConsoleClient::RecursiveList(*owner.SchemeClient, config.Database + "/" + path.c_str());
            for (const auto& entry : list.Entries) {
                if (entry.Type == NScheme::ESchemeEntryType::ColumnTable || entry.Type == NScheme::ESchemeEntryType::Table) {
                    const auto tableDescr = owner.TableClient->GetSession(NTable::TCreateSessionSettings()).GetValueSync().GetSession().DescribeTable(entry.Name).ExtractValueSync().GetTableDescription();
                    auto& params = ArrowCsvParams[entry.Name];
                    params.Columns = tableDescr.GetTableColumns();
                }
            }
        }
    }

    TAsyncStatus WriteDataPortion(NYdbWorkload::IBulkDataGenerator::TDataPortionPtr portion) override {
        if (std::holds_alternative<NYdbWorkload::IBulkDataGenerator::TDataPortion::TSkip>(portion->MutableData())) {
            return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYql::TIssues()));
        }
        if (auto* value = std::get_if<TValue>(&portion->MutableData())) {
            return Owner.TableClient->BulkUpsert(portion->GetTable(), std::move(*value)).Apply(ConvertResult);
        }
        if (auto* value = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TCsv>(&portion->MutableData())) {
            return WriteCsv(portion);
        }
        if (auto* value = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TArrow>(&portion->MutableData())) {
            return Owner.TableClient->RetryOperation([value, portion](NTable::TTableClient& client) {
                return client.BulkUpsert(portion->GetTable(), NTable::EDataFormat::ApacheArrow, value->Data, value->Schema)
                    .Apply(ConvertResult);
            }, RetrySettings);
        }
        Y_FAIL_S("Invalid data portion");
    }

private:
    TAsyncStatus WriteCsv(NYdbWorkload::IBulkDataGenerator::TDataPortionPtr portion) {
        const auto* value = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TCsv>(&portion->MutableData());
        const auto* param = MapFindPtr(ArrowCsvParams, portion->GetTable());
        if (!param) {
            return NThreading::MakeFuture(TStatus(EStatus::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue("Table does not exist: " + portion->GetTable())})));
        }
        auto arrowCsv = NKikimr::NFormats::TArrowCSVTable::Create(param->Columns, true);
        if (!arrowCsv.ok()) {
            return NThreading::MakeFuture(TStatus(EStatus::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(arrowCsv.status().ToString())})));
        }
        Ydb::Formats::CsvSettings csvSettings;
        if (!csvSettings.ParseFromString(value->FormatString)) {
            return NThreading::MakeFuture(TStatus(EStatus::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue("Invalid format string")})));
        };

        auto writeOptions = arrow::ipc::IpcWriteOptions::Defaults();
        constexpr auto codecType = arrow::Compression::type::ZSTD;
        writeOptions.codec = *arrow::util::Codec::Create(codecType);
        TString error;
        if (auto batch = arrowCsv->ReadSingleBatch(value->Data, csvSettings, error)) {
            if (error) {
                return NThreading::MakeFuture(TStatus(EStatus::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(error)})));
            }
            return Owner.TableClient->RetryOperation([
                parquet = NYdb_cli::NArrow::SerializeBatch(batch, writeOptions),
                schema = NYdb_cli::NArrow::SerializeSchema(*batch->schema()),
                portion](NTable::TTableClient& client) {
                return client.BulkUpsert(portion->GetTable(), NTable::EDataFormat::ApacheArrow, parquet, schema)
                    .Apply(ConvertResult);
            }, RetrySettings);
        }
        if (error) {
            return NThreading::MakeFuture(TStatus(EStatus::INTERNAL_ERROR, NYql::TIssues({NYql::TIssue(error)})));
        }
        return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYql::TIssues()));
    }

    static TStatus ConvertResult(const NTable::TAsyncBulkUpsertResult& result) {
        return TStatus(result.GetValueSync());
    }

    struct TArrowCSVParams {
        TVector<NYdb::NTable::TTableColumn> Columns;
    };

    TMap<TString, TArrowCSVParams> ArrowCsvParams;
    NRetry::TRetryOperationSettings RetrySettings;
};

class TWorkloadCommandImport::TUploadCommand::TFileWriter: public IWriter {
public:
    TFileWriter(const TWorkloadCommandImport::TUploadCommand& owner)
        :IWriter(owner)
    {
        Owner.UploadParams.FileOutputPath.ForceDelete();
        Owner.UploadParams.FileOutputPath.MkDirs();
    }

    TAsyncStatus WriteDataPortion(NYdbWorkload::IBulkDataGenerator::TDataPortionPtr portion) override {
        if (std::holds_alternative<NYdbWorkload::IBulkDataGenerator::TDataPortion::TSkip>(portion->MutableData())) {
            return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYql::TIssues()));
        }
        if (auto* value = std::get_if<TValue>(&portion->MutableData())) {
            return NThreading::MakeErrorFuture<TStatus>(std::make_exception_ptr(yexception() << "Not implemented"));
        }
        if (auto* value = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TCsv>(&portion->MutableData())) {
            auto g = Guard(Lock);
            auto [out, created] = GetOutput(portion->GetTable());
            TStringBuf toWrite(value->Data);
            if (!created) {
                TStringBuf firstLine;
                toWrite.ReadLine(firstLine);
            }
            out->Write(toWrite);
            return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYql::TIssues()));
        }
        if (auto* value = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TArrow>(&portion->MutableData())) {
            auto g = Guard(Lock);
            auto [out, created] = GetOutput(portion->GetTable());
            out->Write(value->Data);
            return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYql::TIssues()));
        }
        Y_FAIL_S("Invalid data portion");
    }

private:
    std::pair<TFileOutput*, bool> GetOutput(const TString& table) {
        auto fname = TFsPath(table).Basename();
        if (auto* result = MapFindPtr(CsvOutputs, fname)) {
            return std::make_pair(result->Get(), false);
        }
        auto result = MakeAtomicShared<TFileOutput>(Owner.UploadParams.FileOutputPath / fname);
        CsvOutputs[fname] = result;
        return std::make_pair(result.Get(), true);
    }
    TMap<TString, TAtomicSharedPtr<TFileOutput>> CsvOutputs;
    TAdaptiveLock Lock;
};

void TWorkloadCommandImport::TUploadCommand::ProcessDataGenerator(std::shared_ptr<NYdbWorkload::IBulkDataGenerator> dataGen) noexcept try {
    TAtomic counter = 0;
    for (auto portions = dataGen->GenerateDataPortion(); !portions.empty() && !AtomicGet(ErrorsCount); portions = dataGen->GenerateDataPortion()) {
        TVector<TAsyncStatus> sendingResults;
        for (const auto& data: portions) {
            AtomicIncrement(counter);
            sendingResults.emplace_back(Writer->WriteDataPortion(data).Apply([&counter, g = MakeAtomicShared<TGuard<TFastSemaphore>>(*InFlightSemaphore)](const TAsyncStatus& result) {
                AtomicDecrement(counter);
                return result.GetValueSync();
            }));
        }
        NThreading::WaitAll(sendingResults).Apply([this, sendingResults, portions](const NThreading::TFuture<void>&) {
            bool success = true;
            for (size_t i = 0; i < portions.size(); ++i) {
                const auto& data = portions[i];
                const auto& res = sendingResults[i].GetValueSync();
                auto guard = Guard(Lock);
                if (!res.IsSuccess()) {
                    Cerr << "Bulk upset to " << data->GetTable() << " failed, " << res.GetStatus() << ", " << res.GetIssues().ToString() << Endl;
                    AtomicIncrement(ErrorsCount);
                    success = false;
                } else if (data->GetSize()) {
                    Bar->AddProgress(data->GetSize());
                }
            }
            if (success) {
                for (size_t i = 0; i < portions.size(); ++i) {
                    portions[i]->SetSendResult(sendingResults[i].GetValueSync());
                }
            }
        });
        if (AtomicGet(ErrorsCount)) {
            break;
        }
    }
    while(AtomicGet(counter) > 0) {
        Sleep(TDuration::MilliSeconds(100));
    }
} catch (...) {
    auto g = Guard(Lock);
    Cerr << "Fill table " << dataGen->GetName() << " failed: " << CurrentExceptionMessage() << ", backtrace: ";
    PrintBackTrace();
    AtomicSet(ErrorsCount, 1);
}
}