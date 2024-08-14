#pragma once

#include <thread>
#include <functional>

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/formats.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <util/generic/size_literals.h>

namespace NYdb {

class TDriver;

namespace NOperation {
class TOperationClient;
}
namespace NScheme {
class TSchemeClient;
}
namespace NTable {
class TTableClient;
}
namespace NImport {
class TImportClient;
}

namespace NConsoleClient {

struct TImportFileSettings : public TOperationRequestSettings<TImportFileSettings> {
    using TSelf = TImportFileSettings;

    static constexpr ui64 MaxBytesPerRequest = 8_MB;
    static constexpr const char * DefaultDelimiter = ",";
    static constexpr ui32 MaxRetries = 10000;

    // Allowed values: Csv, Tsv, JsonUnicode, JsonBase64. Default means Csv
    FLUENT_SETTING_DEFAULT(TDuration, OperationTimeout, TDuration::Seconds(5 * 60));
    FLUENT_SETTING_DEFAULT(TDuration, ClientTimeout, OperationTimeout_ + TDuration::Seconds(5));
    FLUENT_SETTING_DEFAULT(EOutputFormat, Format, EOutputFormat::Default);
    FLUENT_SETTING_DEFAULT(ui64, BytesPerRequest, 1_MB);
    FLUENT_SETTING_DEFAULT(ui64, FileBufferSize, 2_MB);
    FLUENT_SETTING_DEFAULT(ui64, MaxInFlightRequests, 100);
    FLUENT_SETTING_DEFAULT(ui64, Threads, std::thread::hardware_concurrency());
    // Settings below are for CSV format only
    FLUENT_SETTING_DEFAULT(ui32, SkipRows, 0);
    FLUENT_SETTING_DEFAULT(bool, Header, false);
    FLUENT_SETTING_DEFAULT(bool, NewlineDelimited, false);
    FLUENT_SETTING_DEFAULT(TString, HeaderRow, "");
    FLUENT_SETTING_DEFAULT(TString, Delimiter, DefaultDelimiter);
    FLUENT_SETTING_DEFAULT(std::optional<TString>, NullValue, std::nullopt);
};

class TImportFileClient {
public:
    explicit TImportFileClient(const TDriver& driver, const TClientCommand::TConfig& rootConfig);
    TImportFileClient(const TImportFileClient&) = delete;

    // Ingest data from the input files to the database table.
    //   fsPaths: vector of paths to input files
    //   dbPath: full path to the database table, including the database path
    //   settings: input data format and operational settings
    TStatus Import(const TVector<TString>& fsPaths, const TString& dbPath, const TImportFileSettings& settings = {});

private:
    std::shared_ptr<NTable::TTableClient> TableClient;

    NTable::TBulkUpsertSettings UpsertSettings;
    NTable::TRetryOperationSettings RetrySettings;

    std::unique_ptr<const NTable::TTableDescription> DbTableInfo;

    std::atomic<ui64> FilesCount;

    static constexpr ui32 VerboseModeReadSize = 1 << 27; // 100 MB

    using ProgressCallbackFunc = std::function<void (ui64, ui64)>;

    TStatus UpsertCsv(IInputStream& input, const TString& dbPath, const TImportFileSettings& settings,
                    std::optional<ui64> inputSizeHint, ProgressCallbackFunc & progressCallback);
    TStatus UpsertCsvByBlocks(const TString& filePath, const TString& dbPath, const TImportFileSettings& settings);
    TAsyncStatus UpsertTValueBuffer(const TString& dbPath, TValueBuilder& builder);

    TStatus UpsertJson(IInputStream &input, const TString &dbPath, const TImportFileSettings &settings,
                    std::optional<ui64> inputSizeHint, ProgressCallbackFunc & progressCallback);

    TStatus UpsertParquet(const TString& filename, const TString& dbPath, const TImportFileSettings& settings,
                    ProgressCallbackFunc & progressCallback);
    TAsyncStatus UpsertParquetBuffer(const TString& dbPath, const TString& buffer, const TString& strSchema);

    TType GetTableType();
    std::map<TString, TType> GetColumnTypes();
    void ValidateTValueUpsertTable();
};

}
}
