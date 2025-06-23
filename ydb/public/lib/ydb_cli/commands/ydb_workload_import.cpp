#include "ydb_workload_import.h"
#include <ydb/library/formats/arrow/csv/table/table.h>
#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/import/cli_arrow_helpers.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/threading/future/async.h>
#include <util/generic/deque.h>
#include <thread>

namespace NYdb::NConsoleClient {

std::unique_ptr<TClientCommand> TWorkloadCommandImport::Create(NYdbWorkload::TWorkloadParams& workloadParams) {
    auto initializers = workloadParams.CreateDataInitializers();
    if (initializers.empty()) {
        return {};
    }
    if (initializers.size() == 1 && initializers.front()->GetName().empty()) {
        return std::make_unique<TUploadCommand>(workloadParams, nullptr, initializers.front());
    }
    return std::unique_ptr<TClientCommand>(new TWorkloadCommandImport(workloadParams, initializers));
}

TWorkloadCommandImport::TWorkloadCommandImport(NYdbWorkload::TWorkloadParams& workloadParams, NYdbWorkload::TWorkloadDataInitializer::TList initializers)
    : TClientCommandTree("import", {}, "Fill tables for workload with data.")
    , UploadParams(workloadParams)
{
    for (auto initializer: initializers) {
        AddCommand(std::make_unique<TUploadCommand>(workloadParams, &UploadParams, initializer));
    }
}

void TWorkloadCommandImport::Config(TConfig& config) {
    TClientCommandTree::Config(config);
    UploadParams.Config(config);
}

void TWorkloadCommandImport::TUploadParams::Config(TConfig& config) {
    config.Opts->AddLongOption('t', "upload-threads", "Number of threads to generate tables content.")
        .Optional().DefaultValue(Threads).StoreResult(&Threads);
    config.Opts->AddLongOption("bulk-size", "Data portion size in rows for upload.")
        .DefaultValue(WorkloadParams.BulkSize).StoreResult(&WorkloadParams.BulkSize);
    config.Opts->AddLongOption("max-in-flight", "Maximum number if data portions that can be simultaneously in process.")
        .DefaultValue(MaxInFlight).StoreResult(&MaxInFlight);
    config.Opts->AddLongOption('f', "file-output-path", "Path to a directory to save tables into as files instead of uploading it to db.")
        .StoreResult(&FileOutputPath);
}

TWorkloadCommandImport::TUploadParams::TUploadParams(NYdbWorkload::TWorkloadParams& workloadParams)
    : Threads(std::thread::hardware_concurrency())
    , WorkloadParams(workloadParams)
{}

void TWorkloadCommandImport::TUploadCommand::Config(TConfig& config) {
    TWorkloadCommandBase::Config(config);
    if (OwnedUploadParams) {
        OwnedUploadParams->Config(config);
    }
    Initializer->ConfigureOpts(config.Opts->GetOpts());
}

TWorkloadCommandImport::TUploadCommand::TUploadCommand(NYdbWorkload::TWorkloadParams& workloadParams, const TUploadParams* uploadParams, NYdbWorkload::TWorkloadDataInitializer::TPtr initializer)
    : TWorkloadCommandBase(initializer->GetName() ? initializer->GetName() : "import", workloadParams, NYdbWorkload::TWorkloadParams::ECommandType::Import, initializer->GetDescription())
    , OwnedUploadParams(uploadParams ? nullptr : new TUploadParams(workloadParams))
    , UploadParams(uploadParams ? *uploadParams : *OwnedUploadParams)
    , Initializer(initializer)
{}

int TWorkloadCommandImport::TUploadCommand::DoRun(NYdbWorkload::IWorkloadQueryGenerator& /*workloadGen*/, TConfig& /*config*/) {
    auto dataGeneratorList = Initializer->GetBulkInitialData();
    AtomicSet(ErrorsCount, 0);
    InFlightSemaphore = MakeHolder<TFastSemaphore>(UploadParams.MaxInFlight);
    if (UploadParams.FileOutputPath.IsDefined()) {
        Writer = MakeHolder<TFileWriter>(*this);
    } else {
        Writer = MakeHolder<TDbWriter>(*this);
    }
    for (auto dataGen : dataGeneratorList) {
        TThreadPoolParams params;
        params.SetCatching(false);
        TThreadPool pool;
        pool.Start(UploadParams.Threads);
        const auto start = Now();
        Cout << "Fill table " << dataGen->GetName() << "..."  << Endl;
        Bar = MakeHolder<TProgressBar>(dataGen->GetSize());
        for (ui32 t = 0; t < UploadParams.Threads; ++t) {
            pool.SafeAddFunc([this, dataGen] () {
                ProcessDataGenerator(dataGen);
            });
        }
        pool.Stop();
        const bool wereErrors = AtomicGet(ErrorsCount);
        with_lock(Lock) {
            Cout << "Fill table " << dataGen->GetName()  << " "<< (wereErrors ? "Failed" : "OK" ) << " " << Bar->GetCurProgress() << " / " << Bar->GetCapacity() << " (" << (Now() - start) << ")" << Endl;
        }
        if (wereErrors) {
            break;
        }
    }
    return AtomicGet(ErrorsCount) ? EXIT_FAILURE : EXIT_SUCCESS;
}
class TWorkloadCommandImport::TUploadCommand::TDbWriter: public IWriter {
public:
    TDbWriter(TWorkloadCommandImport::TUploadCommand& owner)
        : IWriter(owner)
    {
        RetrySettings.RetryUndefined(true);
        RetrySettings.MaxRetries(30);
    }

    TAsyncStatus WriteDataPortion(NYdbWorkload::IBulkDataGenerator::TDataPortionPtr portion) override {
        if (std::holds_alternative<NYdbWorkload::IBulkDataGenerator::TDataPortion::TSkip>(portion->MutableData())) {
            return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues()));
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
        const auto param = GetCSVParams(portion->GetTable());
        if (!param.Status.IsSuccess()) {
            return NThreading::MakeFuture(param.Status);
        }
        auto arrowCsv = NKikimr::NFormats::TArrowCSVTable::Create(param.Columns, true);
        if (!arrowCsv.ok()) {
            return NThreading::MakeFuture(TStatus(EStatus::INTERNAL_ERROR, NYdb::NIssue::TIssues({NYdb::NIssue::TIssue(arrowCsv.status().ToString())})));
        }
        Ydb::Formats::CsvSettings csvSettings;
        if (!csvSettings.ParseFromString(value->FormatString)) {
            return NThreading::MakeFuture(TStatus(EStatus::INTERNAL_ERROR, NYdb::NIssue::TIssues({NYdb::NIssue::TIssue("Invalid format string")})));
        };

        auto writeOptions = arrow::ipc::IpcWriteOptions::Defaults();
        constexpr auto codecType = arrow::Compression::type::ZSTD;
        writeOptions.codec = *arrow::util::Codec::Create(codecType);
        TString error;
        if (auto batch = arrowCsv->ReadSingleBatch(value->Data, csvSettings, error)) {
            if (error) {
                return NThreading::MakeFuture(TStatus(EStatus::INTERNAL_ERROR, NYdb::NIssue::TIssues({NYdb::NIssue::TIssue(error)})));
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
            return NThreading::MakeFuture(TStatus(EStatus::INTERNAL_ERROR, NYdb::NIssue::TIssues({NYdb::NIssue::TIssue(error)})));
        }
        return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues()));
    }

    static TStatus ConvertResult(const NTable::TAsyncBulkUpsertResult& result) {
        return TStatus(result.GetValueSync());
    }

    struct TArrowCSVParams {
        TStatus Status = TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues());
        std::vector<NYdb::NTable::TTableColumn> Columns;
    };

    TArrowCSVParams GetCSVParams(const TString& table) {
        auto g = Guard(CsvParamsLock);
        auto result = ArrowCsvParams.emplace(table, TArrowCSVParams());
        if (result.second) {
            auto session = Owner.TableClient->GetSession(NTable::TCreateSessionSettings()).ExtractValueSync();
            if (!session.IsSuccess()) {
                auto issues = session.GetIssues();
                issues.AddIssue("Cannot create session");
                result.first->second.Status = TStatus(session.GetStatus(), std::move(issues));
            } else {
                const auto tableDescr = session.GetSession().DescribeTable(table).ExtractValueSync();
                if (!tableDescr.IsSuccess()) {
                    auto issues = tableDescr.GetIssues();
                    issues.AddIssue("Cannot descibe table " + table);
                    result.first->second.Status = TStatus(tableDescr.GetStatus(), std::move(issues));
                } else {
                    result.first->second.Columns = tableDescr.GetTableDescription().GetTableColumns();
                }
            }
        }
        return result.first->second;
    }

    TMap<TString, TArrowCSVParams> ArrowCsvParams;
    NRetry::TRetryOperationSettings RetrySettings;
    TAdaptiveLock CsvParamsLock;
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
            return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues()));
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
            return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues()));
        }
        if (auto* value = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TArrow>(&portion->MutableData())) {
            auto g = Guard(Lock);
            auto [out, created] = GetOutput(portion->GetTable());
            out->Write(value->Data);
            return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues()));
        }
        Y_FAIL_S("Invalid data portion");
    }

private:
    std::pair<TFileOutput*, bool> GetOutput(const TString& table) {
        auto fname = TFsPath(table).Basename();
        if (auto* result = MapFindPtr(CsvOutputs, fname)) {
            return std::make_pair(result->Get(), false);
        }
        auto& result = CsvOutputs[fname];
        result = MakeAtomicShared<TFileOutput>(Owner.UploadParams.FileOutputPath / fname);
        return std::make_pair(result.Get(), true);
    }
    TMap<TString, TAtomicSharedPtr<TFileOutput>> CsvOutputs;
    TAdaptiveLock Lock;
};

void TWorkloadCommandImport::TUploadCommand::ProcessDataGenerator(std::shared_ptr<NYdbWorkload::IBulkDataGenerator> dataGen) noexcept {
    TAtomic counter = 0;
    try {
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
    } catch (...) {
        auto g = Guard(Lock);
        Cerr << "Fill table " << dataGen->GetName() << " failed: " << CurrentExceptionMessage() << ", backtrace: ";
        PrintBackTrace();
        AtomicSet(ErrorsCount, 1);
    }
    while(AtomicGet(counter) > 0) {
        Sleep(TDuration::MilliSeconds(100));
    }
}
}
