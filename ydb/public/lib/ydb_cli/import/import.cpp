#include "import.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>

#include <library/cpp/string_utils/csv/csv.h>

#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/folder/path.h>

#include <deque>

namespace NYdb {
namespace NConsoleClient {

namespace {

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
{}

TStatus TImportFileClient::Import(const TString& filePath, const TString& dbPath, const TImportFileSettings& settings) {
    if (filePath.empty()) {
        return MakeStatus(EStatus::BAD_REQUEST, "No file specified");
    }

    TFsPath dataFile(filePath);
    if (!dataFile.Exists()) {
        return MakeStatus(EStatus::BAD_REQUEST,
            TStringBuilder() << "File does not exist: " << filePath);
    }

    if (!dataFile.IsFile()) {
        return MakeStatus(EStatus::BAD_REQUEST,
            TStringBuilder() << "Not a file: " << filePath);
    }

    auto result = NDump::DescribePath(*SchemeClient, dbPath).GetStatus();
    if (result != EStatus::SUCCESS) {
        return MakeStatus(EStatus::SCHEME_ERROR,
            TStringBuilder() <<  "Table does not exist: " << dbPath);
    }

    if (settings.Format_ != NTable::EDataFormat::CSV) {
        return MakeStatus(EStatus::BAD_REQUEST,
            TStringBuilder() << "Unsupported format");
    }

    return UpsertCsv(dataFile, dbPath, settings);
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

TStatus TImportFileClient::UpsertCsv(const TString& dataFile, const TString& dbPath,
                                     const TImportFileSettings& settings) {
    TFileInput input(dataFile, settings.FileBufferSize_);
    TString line;
    TString buffer;

    auto upsertSettings = NTable::TBulkUpsertSettings()
        .OperationTimeout(TDuration::Seconds(30))
        .ClientTimeout(TDuration::Seconds(35));

    auto retrySettings = NTable::TRetryOperationSettings()
        .MaxRetries(10);

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
        upsertSettings.FormatSettings(formatSettings);
    }

    std::deque<TAsyncStatus> inFlightRequests;

    ui64 readSize = 0;
    while (TString line = splitter.ConsumeLine()) {
        buffer += line;
        buffer += '\n';
        readSize += line.size();
        if (buffer.Size() >= settings.BytesPerRequest_) {
            auto status = WaitForQueue(inFlightRequests, settings.MaxInFlightRequests_);
            if (!status.IsSuccess()) {
                return status;
            }

            inFlightRequests.push_back(UpsertCsvBuffer(dbPath, buffer, {}, upsertSettings, retrySettings));

            buffer = headerRow;
        }
    }

    if (!buffer.Empty()) {
        inFlightRequests.push_back(UpsertCsvBuffer(dbPath, buffer, {}, upsertSettings, retrySettings));
    }

    return WaitForQueue(inFlightRequests, 0);
}

TAsyncStatus TImportFileClient::UpsertCsvBuffer(const TString& dbPath, const TString& csv, const TString& header,
                                       const NTable::TBulkUpsertSettings& upsertSettings,
                                       const NTable::TRetryOperationSettings& retrySettings) {
    auto upsert = [dbPath, csv, header, upsertSettings](NYdb::NTable::TTableClient& tableClient) -> TAsyncStatus {
        return tableClient.BulkUpsert(dbPath, NTable::EDataFormat::CSV, csv, header, upsertSettings)
            .Apply([](const NYdb::NTable::TAsyncBulkUpsertResult& bulkUpsertResult) {
                NYdb::TStatus status = bulkUpsertResult.GetValueSync();
                return NThreading::MakeFuture(status);
            });
    };

    return TableClient->RetryOperation(upsert, retrySettings);
}

}
}
