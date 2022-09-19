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

TImportFileClient::TImportFileClient(const TDriver& driver)
    : OperationClient(std::make_shared<NOperation::TOperationClient>(driver))
    , SchemeClient(std::make_shared<NScheme::TSchemeClient>(driver))
    , TableClient(std::make_shared<NTable::TTableClient>(driver))
{
    UpsertSettings
        .OperationTimeout(TDuration::Seconds(30))
        .ClientTimeout(TDuration::Seconds(35));
    RetrySettings
        .MaxRetries(10);
}

TStatus TImportFileClient::Import(const TString& filePath, const TString& dbPath, const TImportFileSettings& settings) {
    if (! filePath.empty()) {
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

    auto result = NDump::DescribePath(*SchemeClient, dbPath).GetStatus();
    if (result != EStatus::SUCCESS) {
        return MakeStatus(EStatus::SCHEME_ERROR,
            TStringBuilder() <<  "Table does not exist: " << dbPath);
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
        default: ;
    }
    return MakeStatus(EStatus::BAD_REQUEST,
        TStringBuilder() << "Unsupported format #" << (int) settings.Format_);
}

namespace {

TStatus WaitForQueue(std::deque<TAsyncStatus>& inFlightRequests, size_t maxQueueSize) {
    while (!inFlightRequests.empty() && inFlightRequests.size() > maxQueueSize) {
        auto status = inFlightRequests.front().ExtractValueSync();
        inFlightRequests.pop_front();
        if (!status.IsSuccess()) {
            return status;
        }
    }

    return MakeStatus();
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
    TString line;
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

    // Do not use csvSettings.skip_rows.
    for (ui32 i = 0; i < settings.SkipRows_; ++i) {
        input.ReadLine(line);
    }

    TString headerRow;
    if (settings.Header_) {
        input.ReadLine(headerRow);
        headerRow += '\n';
        buffer = headerRow;
        csvSettings.set_header(true);
        special = true;
    }

    if (special) {
        TString formatSettings;
        Y_PROTOBUF_SUPPRESS_NODISCARD csvSettings.SerializeToString(&formatSettings);
        UpsertSettings.FormatSettings(formatSettings);
    }

    std::deque<TAsyncStatus> inFlightRequests;

    // TODO: better read
    // * read serveral lines a time
    // * support endlines inside quotes
    // ReadLine() should count quotes for it and stop the line then counter is odd.
    while (size_t sz = input.ReadLine(line)) {
        buffer += line;
        buffer += '\n'; // TODO: keep original endline?

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
