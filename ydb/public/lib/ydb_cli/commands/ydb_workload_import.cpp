#include "ydb_workload_import.h"
#include <ydb/library/formats/arrow/csv/table/table.h>
#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/import/cli_arrow_helpers.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/threading/future/async.h>
#include <util/generic/deque.h>
#include <util/system/condvar.h>
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

class TWorkloadCommandImport::TUploadCommand::TBatchQueue {
public:
    explicit TBatchQueue(size_t capacity)
        : Capacity_(capacity)
    {}

    bool Push(NYdbWorkload::IBulkDataGenerator::TDataPortionPtr item) {
        auto g = Guard(Lock_);
        while (Queue_.size() >= Capacity_ && !Stopped_) {
            NotFull_.Wait(Lock_);
        }
        if (Stopped_) {
            return false;
        }
        Queue_.push_back(std::move(item));
        NotEmpty_.Signal();
        return true;
    }

    NYdbWorkload::IBulkDataGenerator::TDataPortionPtr Pop() {
        auto g = Guard(Lock_);
        while (Queue_.empty() && !Done_) {
            NotEmpty_.Wait(Lock_);
        }
        if (Queue_.empty()) {
            return {};
        }
        auto item = std::move(Queue_.front());
        Queue_.pop_front();
        NotFull_.Signal();
        return item;
    }

    void MarkDone() {
        auto g = Guard(Lock_);
        Done_ = true;
        NotEmpty_.BroadCast();
    }

    void Stop() {
        auto g = Guard(Lock_);
        Stopped_ = true;
        Done_ = true;
        NotFull_.BroadCast();
        NotEmpty_.BroadCast();
    }

private:
    const size_t Capacity_;
    TDeque<NYdbWorkload::IBulkDataGenerator::TDataPortionPtr> Queue_;
    TMutex Lock_;
    TCondVar NotEmpty_;
    TCondVar NotFull_;
    bool Done_ = false;
    bool Stopped_ = false;
};

class TWorkloadCommandImport::TUploadCommand::TFlowController {
public:
    TFlowController(ui32 initialInFlight, ui32 maxInFlight)
        : MaxInFlight_(maxInFlight)
        , CurrentInFlight_(std::min(initialInFlight, maxInFlight))
        , TargetBatchBytes_(25ULL * 1024 * 1024)
        , MinBatchBytes_(1ULL * 1024 * 1024)
        , MaxLatency_(TDuration::Minutes(1))
        , ReduceCooldown_(TDuration::Seconds(5))
    {}

    ui32 GetCurrentInFlight() const { return AtomicGet(CurrentInFlight_); }
    ui64 GetTargetBatchBytes() const { return AtomicGet(TargetBatchBytes_); }
    ui32 GetActiveInFlight() const { return AtomicGet(Inflight_); }
    TDuration GetLastLatency() const { return TDuration::MicroSeconds(AtomicGet(LastLatencyUs_)); }
    ui64 GetLastBatchRows() const { return AtomicGet(LastBatchRows_); }

    ui64 GetTargetBatchRows() const {
        auto avgBytes = AtomicGet(AvgBytesPerRow_);
        if (avgBytes <= 0) {
            return 10000;
        }
        auto target = AtomicGet(TargetBatchBytes_) / avgBytes;
        return std::max<ui64>(100, std::min<ui64>(target, 5000000));
    }

    void UpdateRowSize(ui64 rows, ui64 bytes) {
        if (rows == 0) return;
        auto newAvg = (TAtomic)(bytes / rows);
        auto oldAvg = AtomicGet(AvgBytesPerRow_);
        if (oldAvg <= 0) {
            AtomicSet(AvgBytesPerRow_, newAvg);
        } else {
            AtomicSet(AvgBytesPerRow_, (oldAvg * 7 + newAvg) / 8);
        }
    }

    bool AcquireSlot() {
        while (true) {
            auto current = AtomicGet(Inflight_);
            auto limit = AtomicGet(CurrentInFlight_);
            if (current >= (TAtomic)limit) {
                return false;
            }
            if (AtomicCas(&Inflight_, current + 1, current)) {
                return true;
            }
        }
    }

    void ReleaseSlot() { AtomicDecrement(Inflight_); }

    void OnSuccess(TDuration latency, ui64 batchRows) {
        AtomicSet(LastLatencyUs_, latency.MicroSeconds());
        AtomicSet(LastBatchRows_, batchRows);
        auto now = TInstant::Now();
        with_lock(Lock_) {
            if (latency > MaxLatency_) {
                ReduceInFlight(now);
            } else if (latency < MaxLatency_ / 2 && now - LastReduceTime_ > ReduceCooldown_) {
                IncreaseInFlight();
            }
        }
    }

    void OnOverloaded() {
        auto now = TInstant::Now();
        with_lock(Lock_) { ReduceInFlight(now); }
    }

    void OnBatchTooLarge() {
        auto currentTarget = AtomicGet(TargetBatchBytes_);
        auto newTarget = currentTarget / 2;
        if (newTarget < (TAtomic)MinBatchBytes_) {
            newTarget = MinBatchBytes_;
        }
        AtomicSet(TargetBatchBytes_, newTarget);
    }

private:
    void ReduceInFlight(TInstant now) {
        auto current = AtomicGet(CurrentInFlight_);
        AtomicSet(CurrentInFlight_, std::max<ui32>(1, current * 2 / 3));
        LastReduceTime_ = now;
    }
    void IncreaseInFlight() {
        auto current = AtomicGet(CurrentInFlight_);
        AtomicSet(CurrentInFlight_, std::min<ui32>(MaxInFlight_, current + 1));
    }

    const ui32 MaxInFlight_;
    TAtomic CurrentInFlight_;
    TAtomic TargetBatchBytes_;
    const ui64 MinBatchBytes_;
    const TDuration MaxLatency_;
    const TDuration ReduceCooldown_;
    TAtomic Inflight_ = 0;
    TAtomic LastLatencyUs_ = 0;
    TAtomic LastBatchRows_ = 0;
    TAtomic AvgBytesPerRow_ = 0;
    TInstant LastReduceTime_;
    TAdaptiveLock Lock_;
};

class TWorkloadCommandImport::TUploadCommand::TDbWriter: public IWriter {
public:
    using TDataPortionPtr = NYdbWorkload::IBulkDataGenerator::TDataPortionPtr;

    TDbWriter(TWorkloadCommandImport::TUploadCommand& owner)
        : IWriter(owner)
    {
        RetrySettings.RetryUndefined(true);
        RetrySettings.MaxRetries(30);
        BulkUpsertSettings_.OperationTimeout(TDuration::Minutes(2));
        BulkUpsertSettings_.ClientTimeout(TDuration::Minutes(3));
        WriteOptions_ = arrow::ipc::IpcWriteOptions::Defaults();
        constexpr auto codecType = arrow::Compression::type::ZSTD;
        WriteOptions_.codec = *arrow::util::Codec::Create(codecType);
    }

    TVector<TDataPortionPtr> PrepareArrowPortions(TDataPortionPtr portion) {
        TVector<TDataPortionPtr> result;
        auto* csv = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TCsv>(&portion->MutableData());
        if (!csv) {
            result.push_back(portion);
            return result;
        }

        const auto param = GetCSVParams(portion->GetTable());
        if (!param.Status.IsSuccess()) {
            result.push_back(portion);
            return result;
        }
        auto arrowCsv = NKikimr::NFormats::TArrowCSVTable::Create(param.Columns, true);
        if (!arrowCsv.ok()) {
            result.push_back(portion);
            return result;
        }
        Ydb::Formats::CsvSettings csvSettings;
        if (!csvSettings.ParseFromString(csv->FormatString)) {
            result.push_back(portion);
            return result;
        }

        TString error;
        auto batch = arrowCsv->ReadSingleBatch(csv->Data, csvSettings, error);
        if (!batch || error) {
            result.push_back(portion);
            return result;
        }

        ui64 targetBytes = Owner.FlowCtl ? Owner.FlowCtl->GetTargetBatchBytes() : 25ULL * 1024 * 1024;

        TVector<TSerializedChunk> chunks;
        SplitAndSerialize(batch, targetBytes, chunks);

        if (Owner.FlowCtl && !chunks.empty()) {
            Owner.FlowCtl->UpdateRowSize(chunks[0].NumRows, chunks[0].Parquet.size());
        }

        for (size_t i = 0; i < chunks.size(); ++i) {
            if (i == chunks.size() - 1) {
                portion->MutableData() = NYdbWorkload::IBulkDataGenerator::TDataPortion::TArrow(
                    std::move(chunks[i].Parquet), std::move(chunks[i].Schema));
                result.push_back(portion);
            } else {
                result.push_back(MakeIntrusive<NYdbWorkload::IBulkDataGenerator::TDataPortion>(
                    portion->GetTable(),
                    NYdbWorkload::IBulkDataGenerator::TDataPortion::TArrow(std::move(chunks[i].Parquet), std::move(chunks[i].Schema)),
                    ui64(0)));
            }
        }
        return result;
    }

    TAsyncStatus WriteDataPortion(TDataPortionPtr portion) override {
        if (std::holds_alternative<NYdbWorkload::IBulkDataGenerator::TDataPortion::TSkip>(portion->MutableData())) {
            return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues()));
        }
        if (auto* value = std::get_if<TValue>(&portion->MutableData())) {
            return Owner.TableClient->BulkUpsert(portion->GetTable(), std::move(*value), BulkUpsertSettings_).Apply(ConvertResult);
        }
        if (auto* value = std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TArrow>(&portion->MutableData())) {
            return Owner.TableClient->RetryOperation([this, value, portion](NTable::TTableClient& client) {
                return client.BulkUpsert(portion->GetTable(), NTable::EDataFormat::ApacheArrow, value->Data, value->Schema, BulkUpsertSettings_)
                    .Apply(ConvertResult);
            }, RetrySettings);
        }
        if (std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TCsv>(&portion->MutableData())) {
            return WriteCsv(portion);
        }
        Y_FAIL_S("Invalid data portion");
    }

private:
    struct TSerializedChunk {
        TString Parquet;
        TString Schema;
        i64 NumRows;
    };

    void SplitAndSerialize(std::shared_ptr<arrow::RecordBatch> batch, ui64 targetBytes,
                           TVector<TSerializedChunk>& chunks) {
        auto parquet = NYdb_cli::NArrow::SerializeBatch(batch, WriteOptions_);
        if ((ui64)parquet.size() <= targetBytes || batch->num_rows() <= 1) {
            auto schema = NYdb_cli::NArrow::SerializeSchema(*batch->schema());
            chunks.push_back(TSerializedChunk{std::move(parquet), std::move(schema), batch->num_rows()});
            return;
        }
        i64 mid = batch->num_rows() / 2;
        SplitAndSerialize(batch->Slice(0, mid), targetBytes, chunks);
        SplitAndSerialize(batch->Slice(mid), targetBytes, chunks);
    }

    TAsyncStatus WriteCsv(TDataPortionPtr portion) {
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

        TString error;
        if (auto batch = arrowCsv->ReadSingleBatch(value->Data, csvSettings, error)) {
            if (error) {
                return NThreading::MakeFuture(TStatus(EStatus::INTERNAL_ERROR, NYdb::NIssue::TIssues({NYdb::NIssue::TIssue(error)})));
            }
            auto parquet = NYdb_cli::NArrow::SerializeBatch(batch, WriteOptions_);
            auto schema = NYdb_cli::NArrow::SerializeSchema(*batch->schema());
            return Owner.TableClient->RetryOperation([this, parquet, schema, portion](NTable::TTableClient& client) {
                return client.BulkUpsert(portion->GetTable(), NTable::EDataFormat::ApacheArrow, parquet, schema, BulkUpsertSettings_)
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
    NTable::TBulkUpsertSettings BulkUpsertSettings_;
    arrow::ipc::IpcWriteOptions WriteOptions_;
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
        if (std::get_if<TValue>(&portion->MutableData())) {
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

int TWorkloadCommandImport::TUploadCommand::DoRun(NYdbWorkload::IWorkloadQueryGenerator& /*workloadGen*/, TConfig& /*config*/) {
    auto dataGeneratorList = Initializer->GetBulkInitialData();
    AtomicSet(ErrorsCount, 0);
    AtomicSet(InFlightCounter, 0);
    InFlightSemaphore = MakeHolder<TFastSemaphore>(UploadParams.MaxInFlight);
    if (UploadParams.FileOutputPath.IsDefined()) {
        Writer = MakeHolder<TFileWriter>(*this);
    } else {
        FlowCtl = MakeHolder<TFlowController>(UploadParams.MaxInFlight / 2, UploadParams.MaxInFlight);
        Writer = MakeHolder<TDbWriter>(*this);
    }
    for (auto dataGen : dataGeneratorList) {
        const auto start = Now();
        Cout << "Fill table " << dataGen->GetName() << "..."  << Endl;
        Bar = MakeHolder<TProgressBar>(dataGen->GetSize(), 100);

        TBatchQueue rawQueue(UploadParams.Threads * 2);
        TBatchQueue sendQueue(UploadParams.MaxInFlight * 2);

        std::thread readerThread([this, &dataGen, &rawQueue]() {
            ReaderFunc(dataGen, rawQueue);
        });

        ui32 converterThreads = std::max<ui32>(2, UploadParams.Threads / 2);
        TThreadPool converterPool;
        converterPool.Start(converterThreads);
        for (ui32 t = 0; t < converterThreads; ++t) {
            converterPool.SafeAddFunc([this, &rawQueue, &sendQueue]() {
                try {
                    auto* dbWriter = dynamic_cast<TDbWriter*>(Writer.Get());
                    while (!AtomicGet(ErrorsCount)) {
                        auto data = rawQueue.Pop();
                        if (!data) {
                            break;
                        }
                        if (dbWriter && std::get_if<NYdbWorkload::IBulkDataGenerator::TDataPortion::TCsv>(&data->MutableData())) {
                            auto arrowPortions = dbWriter->PrepareArrowPortions(data);
                            for (auto& p : arrowPortions) {
                                if (!sendQueue.Push(p)) {
                                    return;
                                }
                            }
                        } else {
                            if (!sendQueue.Push(data)) {
                                return;
                            }
                        }
                    }
                } catch (...) {
                    auto g = Guard(Lock);
                    Cerr << "Converter failed: " << CurrentExceptionMessage() << Endl;
                    AtomicSet(ErrorsCount, 1);
                }
            });
        }

        TThreadPool senderPool;
        senderPool.Start(UploadParams.Threads);
        for (ui32 t = 0; t < UploadParams.Threads; ++t) {
            senderPool.SafeAddFunc([this, &sendQueue]() {
                SenderFunc(sendQueue);
            });
        }

        readerThread.join();
        converterPool.Stop();
        sendQueue.MarkDone();
        senderPool.Stop();

        while (AtomicGet(InFlightCounter) > 0) {
            Sleep(TDuration::MilliSeconds(100));
        }

        const bool wereErrors = AtomicGet(ErrorsCount);
        with_lock(Lock) {
            Cout << "Fill table " << dataGen->GetName() << " " << (wereErrors ? "Failed" : "OK")
                 << " " << Bar->GetCurProgress() << " / " << Bar->GetCapacity()
                 << " (" << (Now() - start) << ")" << Endl;
        }
        if (wereErrors) {
            break;
        }
    }

    if (AtomicGet(ErrorsCount) == 0 && Initializer->PostImport() != EXIT_SUCCESS) {
        AtomicIncrement(ErrorsCount);
    }
    return AtomicGet(ErrorsCount) ? EXIT_FAILURE : EXIT_SUCCESS;
}

void TWorkloadCommandImport::TUploadCommand::ReaderFunc(std::shared_ptr<NYdbWorkload::IBulkDataGenerator> dataGen, TBatchQueue& queue) noexcept {
    try {
        for (;;) {
            if (AtomicGet(ErrorsCount)) {
                break;
            }
            if (FlowCtl) {
                UploadParams.WorkloadParams.BulkSize = FlowCtl->GetTargetBatchRows();
            }
            auto portions = dataGen->GenerateDataPortion();
            if (portions.empty()) {
                break;
            }
            for (const auto& data : portions) {
                if (AtomicGet(ErrorsCount)) {
                    break;
                }
                if (!queue.Push(data)) {
                    goto done;
                }
            }
        }
    } catch (...) {
        auto g = Guard(Lock);
        Cerr << "Reader failed: " << CurrentExceptionMessage() << Endl;
        AtomicSet(ErrorsCount, 1);
    }
done:
    queue.MarkDone();
}

void TWorkloadCommandImport::TUploadCommand::SenderFunc(TBatchQueue& queue) noexcept {
    try {
        while (!AtomicGet(ErrorsCount)) {
            auto data = queue.Pop();
            if (!data) {
                break;
            }
            if (FlowCtl) {
                while (!FlowCtl->AcquireSlot() && !AtomicGet(ErrorsCount)) {
                    Sleep(TDuration::MilliSeconds(10));
                }
            }
            if (AtomicGet(ErrorsCount)) {
                break;
            }
            AtomicIncrement(InFlightCounter);
            auto startTime = TInstant::Now();
            Writer->WriteDataPortion(data).Apply(
                [this, startTime, data, g = MakeAtomicShared<TGuard<TFastSemaphore>>(*InFlightSemaphore)](const TAsyncStatus& result) {
                    auto status = result.GetValueSync();
                    ui32 inflightSnapshot = 0;
                    if (FlowCtl) {
                        inflightSnapshot = FlowCtl->GetActiveInFlight();
                        FlowCtl->ReleaseSlot();
                        auto latency = TInstant::Now() - startTime;
                        if (status.IsSuccess()) {
                            FlowCtl->OnSuccess(latency, data->GetSize());
                        } else if (status.GetStatus() == EStatus::OVERLOADED) {
                            FlowCtl->OnOverloaded();
                        }
                    }
                    {
                        auto guard = Guard(Lock);
                        if (!status.IsSuccess()) {
                            if (status.GetStatus() == EStatus::OVERLOADED) {
                                Cerr << "Bulk upsert to " << data->GetTable() << " got OVERLOADED, reducing inflight" << Endl;
                            } else {
                                Cerr << "Bulk upsert to " << data->GetTable() << " failed, " << status.GetStatus() << ", " << status.GetIssues().ToString() << Endl;
                                AtomicIncrement(ErrorsCount);
                            }
                        } else {
                            if (data->GetSize()) {
                                Bar->AddProgress(data->GetSize());
                            }
                            data->SetSendResult(status);
                            if (FlowCtl) {
                                Cerr << "\r  latency=" << FlowCtl->GetLastLatency()
                                     << " inflight=" << inflightSnapshot << "/" << FlowCtl->GetCurrentInFlight()
                                     << " batch_rows=" << FlowCtl->GetLastBatchRows()
                                     << " target_batch=" << (FlowCtl->GetTargetBatchBytes() >> 20) << "MiB"
                                     << "     ";
                                Cerr.Flush();
                            }
                        }
                    }
                    AtomicDecrement(InFlightCounter);
                    return status;
                });
        }
    } catch (...) {
        auto g = Guard(Lock);
        Cerr << "Sender failed: " << CurrentExceptionMessage() << Endl;
        AtomicSet(ErrorsCount, 1);
    }
}
}
