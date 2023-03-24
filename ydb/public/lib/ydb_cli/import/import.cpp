#include "import.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>

#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/folder/path.h>

#include <deque>

#include <library/cpp/string_utils/csv/csv.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/file_reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <ydb/public/lib/ydb_cli/import/cli_arrow_helpers.h>

namespace NYdb {
namespace NConsoleClient {

namespace {

static inline
TStatus MakeStatus(EStatus code = EStatus::SUCCESS, const TString& error = {}) {
    NYql::TIssues issues;
    if (error) {
        issues.AddIssue(NYql::TIssue(error));
    }
    return TStatus(code, std::move(issues));
}

}

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

TStatus TImportFileClient::Import(const TString& filePath, const TString& dbPath, const TImportFileSettings& settings) {
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

    // If the filename passed is empty, read from stdin, else from the file.
    std::unique_ptr<TFileInput> fileInput = filePath.empty() ? nullptr
        : std::make_unique<TFileInput>(filePath, settings.FileBufferSize_);
    IInputStream& input = fileInput ? *fileInput : Cin;

    switch (settings.Format_) {
        case EOutputFormat::Default:
        case EOutputFormat::Csv:
        case EOutputFormat::Tsv:
            return UpsertCsv(input, dbPath, settings);
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
}

namespace {

TStatus WaitForQueue(std::deque<TAsyncStatus>& inFlightRequests, size_t maxQueueSize) {
    std::vector<TStatus> problemResults;
    while (!inFlightRequests.empty() && inFlightRequests.size() > maxQueueSize && problemResults.empty()) {
        NThreading::WaitAny(inFlightRequests).Wait();
        ui32 delta = 0;
        for (ui32 i = 0; i + delta < inFlightRequests.size();) {
            if (inFlightRequests[i].HasValue() || inFlightRequests[i].HasException()) {
                auto status = inFlightRequests[i].ExtractValueSync();
                if (!status.IsSuccess()) {
                    problemResults.emplace_back(status);
                }
                ++delta;
                inFlightRequests[i] = inFlightRequests[inFlightRequests.size() - delta];
            } else {
                ++i;
            }
        }
        inFlightRequests.resize(inFlightRequests.size() - delta);
    }
    if (problemResults.size()) {
        return problemResults.front();
    } else {
        return MakeStatus();
    }
}

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

TStatus TImportFileClient::UpsertCsv(IInputStream& input, const TString& dbPath,
                                     const TImportFileSettings& settings) {
    TString buffer;

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

    NCsvFormat::TLinesSplitter splitter(input);
    TString headerRow;
    if (settings.Header_) {
        headerRow = splitter.ConsumeLine();
        headerRow += '\n';
        buffer = headerRow;
        csvSettings.set_header(true);
        special = true;
    }

    // Do not use csvSettings.skip_rows.
    for (ui32 i = 0; i < settings.SkipRows_; ++i) {
        splitter.ConsumeLine();
    }

    if (special) {
        TString formatSettings;
        Y_PROTOBUF_SUPPRESS_NODISCARD csvSettings.SerializeToString(&formatSettings);
        UpsertSettings.FormatSettings(formatSettings);
    }

    std::deque<TAsyncStatus> inFlightRequests;

    ui32 idx = settings.SkipRows_;
    ui64 readSize = 0;
    const ui32 mb100 = 1 << 27;
    ui64 nextBorder = mb100;
    while (TString line = splitter.ConsumeLine()) {
        buffer += line;
        buffer += '\n';
        readSize += line.size();
        ++idx;
        if (readSize >= nextBorder && RetrySettings.Verbose_) {
            nextBorder += mb100;
            Cerr << "Processed " << 1.0 * readSize / (1 << 20) << "Mb and " << idx << " records" << Endl;
        }
        if (buffer.Size() >= settings.BytesPerRequest_) {
            auto status = WaitForQueue(inFlightRequests, settings.MaxInFlightRequests_);
            if (!status.IsSuccess()) {
                return status;
            }

            inFlightRequests.push_back(UpsertCsvBuffer(dbPath, buffer));
            buffer = headerRow;
        }
    }

    if (!buffer.Empty()) {
        inFlightRequests.push_back(UpsertCsvBuffer(dbPath, buffer));
    }

    return WaitForQueue(inFlightRequests, 0);
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

    std::deque<TAsyncStatus> inFlightRequests;

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

            auto status = WaitForQueue(inFlightRequests, settings.MaxInFlightRequests_);
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

    return WaitForQueue(inFlightRequests, 0);
}

TStatus TImportFileClient::UpsertParquet([[maybe_unused]]const TString& filename, [[maybe_unused]]const TString& dbPath, [[maybe_unused]]const TImportFileSettings& settings) {
    #if defined (_WIN64) || defined (_WIN32) || defined (__WIN32__)
        return MakeStatus(EStatus::BAD_REQUEST, TStringBuilder() << "Not supported on Windows");
    #else
    std::shared_ptr<arrow::io::ReadableFile> infile;
    arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> fileResult = arrow::io::ReadableFile::Open(filename);
    if (!fileResult.ok()) {
        return MakeStatus(EStatus::BAD_REQUEST, TStringBuilder() << "Unable to open parquet file:" << fileResult.status().ToString());
    }
    std::shared_ptr<arrow::io::ReadableFile> readableFile = fileResult.ValueOrDie();

    std::unique_ptr<parquet::arrow::FileReader> fileReader;
    arrow::MemoryPool *pool = arrow::default_memory_pool();

    arrow::Status st;
    st = parquet::arrow::OpenFile(readableFile, pool, &fileReader);
    if (!st.ok()) {
        return MakeStatus(EStatus::BAD_REQUEST, TStringBuilder() << "Error while initializing arrow FileReader: " << st.ToString());
    }

    std::shared_ptr<parquet::FileMetaData> metaData = parquet::ReadMetaData(readableFile);

    i64 numRowGroups = metaData->num_row_groups();

    std::vector<int> row_group_indices(numRowGroups);
    for (i64 i = 0; i < numRowGroups; i++) {
        row_group_indices[i] = i;
    }

    std::shared_ptr<arrow::RecordBatchReader> reader;

    st = fileReader->GetRecordBatchReader(row_group_indices, &reader);
    if (!st.ok()) {
        return MakeStatus(EStatus::BAD_REQUEST, TStringBuilder() << "Error while getting RecordBatchReader: " << st.ToString());
    }

    std::deque<TAsyncStatus> inFlightRequests;

    auto splitUpsertBatch = [this, &inFlightRequests, dbPath, settings](const std::shared_ptr<arrow::RecordBatch> &recordBatch){
        std::vector<std::shared_ptr<arrow::RecordBatch>> slicedRecordBatches;
        std::deque<std::shared_ptr<arrow::RecordBatch>> batchesDeque;
        size_t totalSize = NYdb_cli::NArrow::GetBatchDataSize(recordBatch);

        size_t sliceCnt = totalSize / (size_t)settings.BytesPerRequest_;
        if (totalSize % settings.BytesPerRequest_ != 0) {
            sliceCnt++;
        }
        int64_t rowsInSlice = recordBatch->num_rows() / sliceCnt;

        for (int64_t currentRow = 0; currentRow < recordBatch->num_rows(); currentRow += rowsInSlice) {
            auto nextSlice = (currentRow + rowsInSlice < recordBatch->num_rows()) ? recordBatch->Slice(currentRow, rowsInSlice) : recordBatch->Slice(currentRow);
            batchesDeque.push_back(nextSlice);
        }

        while (!batchesDeque.empty()) {
            std::shared_ptr<arrow::RecordBatch> nextBatch = batchesDeque.front();
            batchesDeque.pop_front();
            if (NYdb_cli::NArrow::GetBatchDataSize(nextBatch) < settings.BytesPerRequest_) {
                slicedRecordBatches.push_back(nextBatch);
            }
            else {
                std::shared_ptr<arrow::RecordBatch> left = nextBatch->Slice(0, nextBatch->num_rows() / 2);
                std::shared_ptr<arrow::RecordBatch> right = nextBatch->Slice(nextBatch->num_rows() / 2);
                batchesDeque.push_front(right);
                batchesDeque.push_front(left);
            }
        }
        auto schema = recordBatch->schema();
        TString strSchema = NYdb_cli::NArrow::SerializeSchema(*schema);
        for (size_t i = 0; i < slicedRecordBatches.size(); i++) {
            TString buffer = NYdb_cli::NArrow::SerializeBatchNoCompression(slicedRecordBatches[i]);
            auto status = WaitForQueue(inFlightRequests, settings.MaxInFlightRequests_);
            if (!status.IsSuccess()) {
                return status;
            }

            inFlightRequests.push_back(UpsertParquetBuffer(dbPath, buffer, strSchema));
        }

        return MakeStatus(EStatus::SUCCESS);
    };

    std::shared_ptr<arrow::RecordBatch> currentBatch;
    st = reader->ReadNext(&currentBatch);
    if (!st.ok()) {
        return MakeStatus(EStatus::BAD_REQUEST, TStringBuilder() << "Error while reading next RecordBatch" << st.ToString());
    }

    while(currentBatch) {
        TStatus upsertStatus = splitUpsertBatch(currentBatch);
        if (!upsertStatus.IsSuccess()) {
            return upsertStatus;
        }
        st = reader->ReadNext(&currentBatch);
        if (!st.ok()) {
            return MakeStatus(EStatus::BAD_REQUEST, TStringBuilder() << "Error while reading next RecordBatch" << st.ToString());
        }
    }

    return WaitForQueue(inFlightRequests, 0);
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
