#pragma once

#include <semaphore>
#include <thread>
#include <functional>

#include <util/thread/pool.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/formats.h>
#include <ydb-cpp-sdk/client/types/status/status.h>
#include <ydb-cpp-sdk/client/types/fluent_settings_helpers.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <util/generic/size_literals.h>

namespace NYdb {

inline namespace Dev {
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
    FLUENT_SETTING_DEFAULT(EDataFormat, Format, EDataFormat::Default);
    FLUENT_SETTING_DEFAULT(EBinaryStringEncoding, BinaryStringsEncoding, EBinaryStringEncoding::Unicode);
    FLUENT_SETTING_DEFAULT(ui64, BytesPerRequest, 1_MB);
    FLUENT_SETTING_DEFAULT(ui64, FileBufferSize, 2_MB);
    FLUENT_SETTING_DEFAULT(ui64, MaxInFlightRequests, 100);
    // Main thread that reads input file is CPU intensive so make room for it too
    FLUENT_SETTING_DEFAULT(ui64, Threads, std::thread::hardware_concurrency() > 1 ? std::thread::hardware_concurrency() - 1 : 1);
    // Settings below are for CSV format only
    FLUENT_SETTING_DEFAULT(ui32, SkipRows, 0);
    FLUENT_SETTING_DEFAULT(bool, Header, false);
    FLUENT_SETTING_DEFAULT(bool, NewlineDelimited, false);
    FLUENT_SETTING_DEFAULT(TString, HeaderRow, "");
    FLUENT_SETTING_DEFAULT(TString, Delimiter, DefaultDelimiter);
    FLUENT_SETTING_DEFAULT(std::optional<TString>, NullValue, std::nullopt);
    FLUENT_SETTING_DEFAULT(bool, Verbose, false);
};

class TImportFileClient {
public:
    explicit TImportFileClient(const TDriver& driver, const TClientCommand::TConfig& rootConfig,
                               const TImportFileSettings& settings = {});
    TImportFileClient(const TImportFileClient&) = delete;

    // Ingest data from the input files to the database table.
    //   fsPaths: vector of paths to input files
    //   dbPath: full path to the database table, including the database path
    //   settings: input data format and operational settings
    TStatus Import(const TVector<TString>& filePaths, const TString& dbPath);

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

}
}
