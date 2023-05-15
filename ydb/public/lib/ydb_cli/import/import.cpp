#include "import.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>
#include <ydb/public/lib/ydb_cli/import/cli_arrow_helpers.h>

#include <library/cpp/string_utils/csv/csv.h>
#include <library/cpp/threading/future/async.h>

#include <util/folder/path.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/stream/length.h>
#include <util/string/builder.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/file_reader.h>

#include <stack>

#if defined(_win32_)
#include <windows.h>
#elif defined(_unix_)
#include <unistd.h>
#endif


namespace NYdb {
namespace NConsoleClient {
namespace {

inline
TStatus MakeStatus(EStatus code = EStatus::SUCCESS, const TString& error = {}) {
    NYql::TIssues issues;
    if (error) {
        issues.AddIssue(NYql::TIssue(error));
    }
    return TStatus(code, std::move(issues));
}

TStatus WaitForQueue(const size_t maxQueueSize, std::vector<TAsyncStatus>& inFlightRequests) {
    while (!inFlightRequests.empty() && inFlightRequests.size() >= maxQueueSize) {
        NThreading::WaitAny(inFlightRequests).Wait();
        ui32 delta = 0;
        for (ui32 i = 0; i + delta < inFlightRequests.size();) {
            if (inFlightRequests[i].HasValue() || inFlightRequests[i].HasException()) {
                auto status = inFlightRequests[i].ExtractValueSync();
                if (!status.IsSuccess()) {
                    return status;
                }
                ++delta;
                inFlightRequests[i] = inFlightRequests[inFlightRequests.size() - delta];
            } else {
                ++i;
            }
        }
        inFlightRequests.resize(inFlightRequests.size() - delta);
    }

    return MakeStatus();
}

FHANDLE GetStdinFileno() {
#if defined(_win32_)
    return GetStdHandle(STD_INPUT_HANDLE);
#elif defined(_unix_)
    return STDIN_FILENO;
#endif
}

class TMaxInflightGetter {
public:
    TMaxInflightGetter(ui64 totalMaxInFlight, std::atomic<ui64>& filesCount) 
        : TotalMaxInFlight(totalMaxInFlight)
        , FilesCount(filesCount) {
    }

    ~TMaxInflightGetter() {
        --FilesCount;
    }

    ui64 GetCurrentMaxInflight() const {
        return (TotalMaxInFlight - 1) / FilesCount + 1; // round up
    }

private:
    ui64 TotalMaxInFlight;
    std::atomic<ui64>& FilesCount;
};

class TCsvFileReader {
private:
    class TFileChunk {
    public:
        TFileChunk(TFile file, THolder<IInputStream>&& stream, ui64 size = std::numeric_limits<ui64>::max())
            : File(file)
            , Stream(std::move(stream))
            , CountStream(MakeHolder<TCountingInput>(Stream.Get()))
            , Size(size) {
        }

        bool ConsumeLine(TString& line) {
            ui64 prevCount = CountStream->Counter();
            line = NCsvFormat::TLinesSplitter(*CountStream).ConsumeLine();
            if (prevCount == CountStream->Counter() || prevCount >= Size) {
                return false;
            }
            return true;
        }

        ui64 GetReadCount() const {
            return CountStream->Counter();
        }

    private:
        TFile File;
        THolder<IInputStream> Stream;
        THolder<TCountingInput> CountStream;
        ui64 Size;
    };

public:
    TCsvFileReader(const TString& filePath, const TImportFileSettings& settings, TString& headerRow, 
                   TMaxInflightGetter& inFlightGetter) {
        TFile file;
        if (filePath) {
            file = TFile(filePath, RdOnly);
        } else {
            file = TFile(GetStdinFileno());
        }
        auto input = MakeHolder<TFileInput>(file);
        TCountingInput countInput(input.Get());

        if (settings.Header_) {
            headerRow = NCsvFormat::TLinesSplitter(countInput).ConsumeLine();
        }
        for (ui32 i = 0; i < settings.SkipRows_; ++i) {
            NCsvFormat::TLinesSplitter(countInput).ConsumeLine();
        }
        i64 skipSize = countInput.Counter();

        MaxInFlight = inFlightGetter.GetCurrentMaxInflight();
        i64 fileSize = file.GetLength();
        if (filePath.empty() || fileSize == -1) {
            SplitCount = 1;
            Chunks.emplace_back(file, std::move(input));
            return;
        }

        SplitCount = Min(MaxInFlight, (fileSize - skipSize) / settings.BytesPerRequest_ + 1);
        i64 chunkSize = (fileSize - skipSize) / SplitCount;
        if (chunkSize == 0) {
            SplitCount = 1;
            chunkSize = fileSize - skipSize;
        }

        i64 curPos = skipSize;
        i64 seekPos = skipSize;
        Chunks.reserve(SplitCount);
        TString temp;
        file = TFile(filePath, RdOnly);
        file.Seek(seekPos, sSet);
        THolder<TFileInput> stream = MakeHolder<TFileInput>(file);
        for (size_t i = 0; i < SplitCount; ++i) {
            seekPos += chunkSize;
            i64 nextPos = seekPos;
            auto nextFile = TFile(filePath, RdOnly);
            auto nextStream = MakeHolder<TFileInput>(nextFile);
            nextFile.Seek(seekPos, sSet);
            if (seekPos > 0) {
                nextFile.Seek(-1, sCur);
                nextPos += nextStream->ReadLine(temp);
            }

            Chunks.emplace_back(file, std::move(stream), nextPos - curPos);
            file = std::move(nextFile);
            stream = std::move(nextStream);
            curPos = nextPos;
        }
    }

    TFileChunk& GetChunk(size_t threadId) {
        if (threadId >= Chunks.size()) {
            throw yexception() << "File chunk number is too big";
        }
        return Chunks[threadId];
    }

    size_t GetThreadLimit(size_t thread_id) const {
        return MaxInFlight / SplitCount + (thread_id < MaxInFlight % SplitCount ? 1 : 0);
    }

    size_t GetSplitCount() const {
        return SplitCount;
    }

private:
    TVector<TFileChunk> Chunks;
    size_t SplitCount;
    size_t MaxInFlight;
};

} // namespace

TImportFileClient::TImportFileClient(const TDriver& driver, const TClientCommand::TConfig& rootConfig)
    : OperationClient(std::make_shared<NOperation::TOperationClient>(driver))
    , SchemeClient(std::make_shared<NScheme::TSchemeClient>(driver))
    , TableClient(std::make_shared<NTable::TTableClient>(driver))
{
    UpsertSettings
        .OperationTimeout(TDuration::Seconds(TImportFileSettings::OperationTimeoutSec))
        .ClientTimeout(TDuration::Seconds(TImportFileSettings::ClientTimeoutSec));
    RetrySettings
        .MaxRetries(TImportFileSettings::MaxRetries)
        .Idempotent(true)
        .Verbose(rootConfig.IsVerbose());
}

TStatus TImportFileClient::Import(const TVector<TString>& filePaths, const TString& dbPath, const TImportFileSettings& settings) {
    FilesCount = filePaths.size();
    auto pool = CreateThreadPool(FilesCount);
    TVector<NThreading::TFuture<TStatus>> asyncResults;
    switch (settings.Format_) {
        case EOutputFormat::Default:
        case EOutputFormat::Csv:
        case EOutputFormat::Tsv:
            SetupUpsertSettingsCsv(settings);
            break;
        case EOutputFormat::Json:
        case EOutputFormat::JsonUnicode:
        case EOutputFormat::JsonBase64:
        case EOutputFormat::Parquet:
            break;
        default: 
            return MakeStatus(EStatus::BAD_REQUEST,
                        TStringBuilder() << "Unsupported format #" << (int) settings.Format_);
    }

    for (const auto& filePath : filePaths) {
        auto func = [this, &filePath, &dbPath, &settings] {
            if (!filePath.empty()) {
                const TFsPath dataFile(filePath);
                if (!dataFile.Exists()) {
                    return MakeStatus(EStatus::BAD_REQUEST,
                        TStringBuilder() << "File does not exist: " << filePath);
                }
                if (!dataFile.IsFile()) {
                    return MakeStatus(EStatus::BAD_REQUEST,
                        TStringBuilder() << "Not a file: " << filePath);
                }
            }

            if (settings.Format_ == EOutputFormat::Tsv) {
                if (settings.Delimiter_ != "\t") {
                    return MakeStatus(EStatus::BAD_REQUEST,
                        TStringBuilder() << "Illegal delimiter for TSV format, only tab is allowed");
                }
            }

            auto result = NDump::DescribePath(*SchemeClient, dbPath);
            auto resultStatus = result.GetStatus();

            if (resultStatus != EStatus::SUCCESS) {
                return MakeStatus(EStatus::SCHEME_ERROR,
                    TStringBuilder() <<  result.GetIssues().ToString() << dbPath);
            }

            std::unique_ptr<TFileInput> fileInput = filePath.empty() ? nullptr
                : std::make_unique<TFileInput>(filePath, settings.FileBufferSize_);
            IInputStream& input = fileInput ? *fileInput : Cin;
            switch (settings.Format_) {
                case EOutputFormat::Default:
                case EOutputFormat::Csv:
                case EOutputFormat::Tsv:
                    if (settings.NewlineDelimited_) {
                        return UpsertCsvByBlocks(filePath, dbPath, settings);
                    } else {
                        return UpsertCsv(input, dbPath, settings);
                    }
                case EOutputFormat::Json:
                case EOutputFormat::JsonUnicode:
                case EOutputFormat::JsonBase64:
                    return UpsertJson(input, dbPath, settings);
                case EOutputFormat::Parquet:
                    return UpsertParquet(filePath, dbPath, settings);
                default: ;
            }
            return MakeStatus(EStatus::BAD_REQUEST,
                        TStringBuilder() << "Unsupported format #" << (int) settings.Format_);
        };

        asyncResults.push_back(NThreading::Async(std::move(func), *pool));
    }

    NThreading::WaitAll(asyncResults).GetValueSync();
    for (const auto& asyncResult : asyncResults) {
        auto result = asyncResult.GetValueSync();
        if (!result.IsSuccess()) {
            return result;
        }
    }
    return MakeStatus(EStatus::SUCCESS);
}

inline
TAsyncStatus TImportFileClient::UpsertCsvBuffer(const TString& dbPath, const TString& buffer) {
    auto upsert = [this, dbPath, buffer](NYdb::NTable::TTableClient& tableClient) -> TAsyncStatus {
        return tableClient.BulkUpsert(dbPath, NTable::EDataFormat::CSV, buffer, {}, UpsertSettings)
            .Apply([](const NYdb::NTable::TAsyncBulkUpsertResult& bulkUpsertResult) {
                NYdb::TStatus status = bulkUpsertResult.GetValueSync();
                return NThreading::MakeFuture(status);
            });
    };
    return TableClient->RetryOperation(upsert, RetrySettings);
}

void TImportFileClient::SetupUpsertSettingsCsv(const TImportFileSettings& settings) {
    Ydb::Formats::CsvSettings csvSettings;
    bool special = false;
    if (settings.Delimiter_ != settings.DefaultDelimiter) {
        csvSettings.set_delimiter(settings.Delimiter_);
        special = true;
    }

    if (settings.NullValue_.size()) {
        csvSettings.set_null_value(settings.NullValue_);
        special = true;
    }

    if (settings.Header_ || settings.HeaderRow_) {
        csvSettings.set_header(true);
        special = true;
    }

    if (special) {
        TString formatSettings;
        Y_PROTOBUF_SUPPRESS_NODISCARD csvSettings.SerializeToString(&formatSettings);
        UpsertSettings.FormatSettings(formatSettings);
    }
}

TStatus TImportFileClient::UpsertCsv(IInputStream& input, const TString& dbPath,
                                     const TImportFileSettings& settings) {
    TString buffer;

    TMaxInflightGetter inFlightGetter(settings.MaxInFlightRequests_, FilesCount);
    
    TCountingInput countInput(&input);
    NCsvFormat::TLinesSplitter splitter(countInput);
    TString headerRow;
    bool RemoveLastDelimiter = false;
    if (settings.Header_ || settings.HeaderRow_) {
        if (settings.Header_) {
            headerRow = splitter.ConsumeLine();
        }
        if (settings.HeaderRow_) {
            headerRow = settings.HeaderRow_;
        }
        if (headerRow.EndsWith(settings.Delimiter_)) {
            RemoveLastDelimiter = true;
            headerRow.erase(headerRow.Size() - settings.Delimiter_.Size());
        }
        headerRow += '\n';
        buffer = headerRow;
    }

    // Do not use csvSettings.skip_rows.
    for (ui32 i = 0; i < settings.SkipRows_; ++i) {
        splitter.ConsumeLine();
    }

    std::vector<TAsyncStatus> inFlightRequests;

    ui32 idx = settings.SkipRows_;
    ui64 readSize = 0;
    ui64 nextBorder = VerboseModeReadSize;
    while (TString line = splitter.ConsumeLine()) {
        readSize += line.size();
        if (RemoveLastDelimiter) {
            if (!line.EndsWith(settings.Delimiter_)) {
                return MakeStatus(EStatus::BAD_REQUEST, 
                        "According to the header, lines should end with a delimiter");
            }
            line.erase(line.Size() - settings.Delimiter_.Size());
        }
        buffer += line;
        buffer += '\n';
        ++idx;
        if (readSize >= nextBorder && RetrySettings.Verbose_) {
            nextBorder += VerboseModeReadSize;
            Cerr << "Processed " << 1.0 * readSize / (1 << 20) << "Mb and " << idx << " records" << Endl;
        }
        if (buffer.Size() >= settings.BytesPerRequest_) {
            auto status = WaitForQueue(inFlightGetter.GetCurrentMaxInflight(), inFlightRequests);
            if (!status.IsSuccess()) {
                return status;
            }

            inFlightRequests.push_back(UpsertCsvBuffer(dbPath, buffer));
            buffer = headerRow;
        }
    }

    if (!buffer.Empty() && countInput.Counter() > 0) {
        inFlightRequests.push_back(UpsertCsvBuffer(dbPath, buffer));
    }

    return WaitForQueue(0, inFlightRequests);
}

TStatus TImportFileClient::UpsertCsvByBlocks(const TString& filePath, const TString& dbPath, const TImportFileSettings& settings) {
    TMaxInflightGetter inFlightGetter(settings.MaxInFlightRequests_, FilesCount);
    TString headerRow;
    TCsvFileReader splitter(filePath, settings, headerRow, inFlightGetter);
    bool RemoveLastDelimiter = false;

    if (settings.Header_ || settings.HeaderRow_) {
        if (settings.HeaderRow_) {
            headerRow = settings.HeaderRow_;
        }
        if (headerRow.EndsWith(settings.Delimiter_)) {
            RemoveLastDelimiter = true;
            headerRow.erase(headerRow.Size() - settings.Delimiter_.Size());
        }
        headerRow += '\n';
    }

    TVector<TAsyncStatus> threadResults(splitter.GetSplitCount());
    THolder<IThreadPool> pool = CreateThreadPool(splitter.GetSplitCount());
    for (size_t threadId = 0; threadId < splitter.GetSplitCount(); ++threadId) {
        auto loadCsv = [this, &settings, &headerRow, &splitter, &dbPath, threadId, RemoveLastDelimiter] () {
            std::vector<TAsyncStatus> inFlightRequests;
            TString buffer;
            buffer = headerRow;
            ui32 idx = settings.SkipRows_;
            ui64 readSize = 0;
            ui64 nextBorder = VerboseModeReadSize;
            TAsyncStatus status;
            TString line;
            while (splitter.GetChunk(threadId).ConsumeLine(line)) {
                readSize += line.size();
                if (RemoveLastDelimiter) {
                    if (!line.EndsWith(settings.Delimiter_)) {
                        return MakeStatus(EStatus::BAD_REQUEST, 
                                "According to the header, lines should end with a delimiter");
                    }
                    line.erase(line.Size() - settings.Delimiter_.Size());
                }
                buffer += line;
                buffer += '\n';
                ++idx;
                if (readSize >= nextBorder && RetrySettings.Verbose_) {
                    nextBorder += VerboseModeReadSize;
                    TStringBuilder builder;
                    builder << "Processed " << 1.0 * readSize / (1 << 20) << "Mb and " << idx << " records" << Endl;
                    Cerr << builder;
                }
                if (buffer.Size() >= settings.BytesPerRequest_) {
                    auto status = WaitForQueue(splitter.GetThreadLimit(threadId), inFlightRequests);
                    if (!status.IsSuccess()) {
                        return status;
                    }

                    inFlightRequests.push_back(UpsertCsvBuffer(dbPath, buffer));
                    buffer = headerRow;
                }
            }

            if (!buffer.Empty() && splitter.GetChunk(threadId).GetReadCount() != 0) {
                inFlightRequests.push_back(UpsertCsvBuffer(dbPath, buffer));
            }

            return WaitForQueue(0, inFlightRequests);
        };
        threadResults[threadId] = NThreading::Async(loadCsv, *pool);
    }
    NThreading::WaitAll(threadResults).Wait();
    for (size_t i = 0; i < splitter.GetSplitCount(); ++i) {
        if (!threadResults[i].GetValueSync().IsSuccess()) {
            return threadResults[i].GetValueSync();
        }
    }
    return MakeStatus();
}

inline
TAsyncStatus TImportFileClient::UpsertJsonBuffer(const TString& dbPath, TValueBuilder& builder) {
    auto upsert = [this, dbPath, rows = builder.Build()]
            (NYdb::NTable::TTableClient& tableClient) -> TAsyncStatus {
        NYdb::TValue r = rows;
        return tableClient.BulkUpsert(dbPath, std::move(r), UpsertSettings)
            .Apply([](const NYdb::NTable::TAsyncBulkUpsertResult& bulkUpsertResult) {
                NYdb::TStatus status = bulkUpsertResult.GetValueSync();
                return NThreading::MakeFuture(status);
            });
    };
    return TableClient->RetryOperation(upsert, RetrySettings);
}

TStatus TImportFileClient::UpsertJson(IInputStream& input, const TString& dbPath,
                                      const TImportFileSettings& settings) {
    NTable::TCreateSessionResult sessionResult = TableClient->GetSession(NTable::TCreateSessionSettings()).GetValueSync();
    if (! sessionResult.IsSuccess())
        return sessionResult;
    NTable::TDescribeTableResult tableResult = sessionResult.GetSession().DescribeTable(dbPath).GetValueSync();
    if (! tableResult.IsSuccess())
        return tableResult;

    const TType tableType = GetTableType(tableResult.GetTableDescription());
    const NYdb::EBinaryStringEncoding stringEncoding =
        (settings.Format_==EOutputFormat::JsonBase64) ? NYdb::EBinaryStringEncoding::Base64 :
            NYdb::EBinaryStringEncoding::Unicode;

    std::vector<TAsyncStatus> inFlightRequests;
    TMaxInflightGetter inFlightGetter(settings.MaxInFlightRequests_, FilesCount);

    size_t currentSize = 0;
    size_t currentRows = 0;
    auto currentBatch = std::make_unique<TValueBuilder>();
    currentBatch->BeginList();

    TString line;
    while (size_t sz = input.ReadLine(line)) {
        currentBatch->AddListItem(JsonToYdbValue(line, tableType, stringEncoding));
        currentSize += line.Size();
        currentRows += 1;

        if (currentSize >= settings.BytesPerRequest_) {
            currentBatch->EndList();

            auto status = WaitForQueue(inFlightGetter.GetCurrentMaxInflight(), inFlightRequests);
            if (!status.IsSuccess()) {
                return status;
            }

            inFlightRequests.push_back(UpsertJsonBuffer(dbPath, *currentBatch));

            currentBatch = std::make_unique<TValueBuilder>();
            currentBatch->BeginList();
            currentSize = 0;
            currentRows = 0;
        }
    }

    if (currentRows > 0) {
        currentBatch->EndList();
        inFlightRequests.push_back(UpsertJsonBuffer(dbPath, *currentBatch));
    }

    return WaitForQueue(0, inFlightRequests);
}

TStatus TImportFileClient::UpsertParquet([[maybe_unused]] const TString& filename,
                                         [[maybe_unused]] const TString& dbPath,
                                         [[maybe_unused]] const TImportFileSettings& settings) {
#if defined(_WIN64) || defined(_WIN32) || defined(__WIN32__)
    return MakeStatus(EStatus::BAD_REQUEST, TStringBuilder() << "Not supported on Windows");
#else
    std::shared_ptr<arrow::io::ReadableFile> infile;
    arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> fileResult = arrow::io::ReadableFile::Open(filename);
    if (!fileResult.ok()) {
        return MakeStatus(EStatus::BAD_REQUEST, TStringBuilder() << "Unable to open parquet file:" << fileResult.status().ToString());
    }
    std::shared_ptr<arrow::io::ReadableFile> readableFile = fileResult.ValueOrDie();

    std::unique_ptr<parquet::arrow::FileReader> fileReader;

    arrow::Status st;
    st = parquet::arrow::OpenFile(readableFile, arrow::default_memory_pool(), &fileReader);
    if (!st.ok()) {
        return MakeStatus(EStatus::BAD_REQUEST, TStringBuilder() << "Error while initializing arrow FileReader: " << st.ToString());
    }

    const i64 numRowGroups = parquet::ReadMetaData(readableFile)->num_row_groups();

    std::vector<int> row_group_indices(numRowGroups);
    for (i64 i = 0; i < numRowGroups; i++) {
        row_group_indices[i] = i;
    }

    std::unique_ptr<arrow::RecordBatchReader> reader;

    st = fileReader->GetRecordBatchReader(row_group_indices, &reader);
    if (!st.ok()) {
        return MakeStatus(EStatus::BAD_REQUEST, TStringBuilder() << "Error while getting RecordBatchReader: " << st.ToString());
    }

    std::vector<TAsyncStatus> inFlightRequests;
    TMaxInflightGetter inFlightGetter(settings.MaxInFlightRequests_, FilesCount);

    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;

        st = reader->ReadNext(&batch);
        if (!st.ok()) {
            return MakeStatus(EStatus::BAD_REQUEST, TStringBuilder() << "Error while reading next RecordBatch" << st.ToString());
        }
        // The read function will return null at the end of data stream.
        if (!batch) {
            break;
        }

        const TString strSchema = NYdb_cli::NArrow::SerializeSchema(*batch->schema());
        const size_t totalSize = NYdb_cli::NArrow::GetBatchDataSize(batch);
        const size_t sliceCount =
            (totalSize / (size_t)settings.BytesPerRequest_) + (totalSize % settings.BytesPerRequest_ != 0 ? 1 : 0);
        const i64 rowsInSlice = batch->num_rows() / sliceCount;

        for (i64 currentRow = 0; currentRow < batch->num_rows(); currentRow += rowsInSlice) {
            std::stack<std::shared_ptr<arrow::RecordBatch>> rowsToSend;

            if (currentRow + rowsInSlice < batch->num_rows()) {
                rowsToSend.push(batch->Slice(currentRow, rowsInSlice));
            } else {
                rowsToSend.push(batch->Slice(currentRow));
            }

            do {
                const auto rows = rowsToSend.top();

                rowsToSend.pop();
                // Nothing to send. Continue.
                if (rows->num_rows() == 0) {
                    continue;
                }
                // Logarithmic approach to find number of rows fit into the byte limit.
                if (rows->num_rows() == 1 || NYdb_cli::NArrow::GetBatchDataSize(rows) < settings.BytesPerRequest_) {
                    // Single row or fits into the byte limit.
                    auto status = WaitForQueue(inFlightGetter.GetCurrentMaxInflight(), inFlightRequests);
                    if (!status.IsSuccess()) {
                        return status;
                    }

                    inFlightRequests.push_back(UpsertParquetBuffer(dbPath, NYdb_cli::NArrow::SerializeBatchNoCompression(rows), strSchema));
                } else {
                    // Split current slice.
                    rowsToSend.push(rows->Slice(rows->num_rows() / 2));
                    rowsToSend.push(rows->Slice(0, rows->num_rows() / 2));
                }
            } while (!rowsToSend.empty());
        }
    }

    return WaitForQueue(0, inFlightRequests);
#endif
}

inline
TAsyncStatus TImportFileClient::UpsertParquetBuffer(const TString& dbPath, const TString& buffer, const TString& strSchema) {
    auto upsert = [this, dbPath, buffer, strSchema](NYdb::NTable::TTableClient& tableClient) -> TAsyncStatus {
        return tableClient.BulkUpsert(dbPath, NTable::EDataFormat::ApacheArrow, buffer, strSchema, UpsertSettings)
            .Apply([](const NYdb::NTable::TAsyncBulkUpsertResult& bulkUpsertResult) {
                NYdb::TStatus status = bulkUpsertResult.GetValueSync();
                return NThreading::MakeFuture(status);
            });
        };
    return TableClient->RetryOperation(upsert, RetrySettings);
}

TType TImportFileClient::GetTableType(const NTable::TTableDescription& tableDescription) {
    TTypeBuilder typeBuilder;
    typeBuilder.BeginStruct();
    const auto& columns = tableDescription.GetTableColumns();
    for (auto it = columns.begin(); it!=columns.end(); it++) {
        typeBuilder.AddMember((*it).Name, (*it).Type);
    }
    typeBuilder.EndStruct();
    return typeBuilder.Build();
}

}
}
