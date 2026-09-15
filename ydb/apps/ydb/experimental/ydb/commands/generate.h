#pragma once

#include <thread>
#include <functional>

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/formats.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/size_literals.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

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

struct TGenerateSettings : public TOperationRequestSettings<TGenerateSettings> {
    using TSelf = TGenerateSettings;

    static constexpr ui64 MaxBytesPerRequest = 8_MB;
    static constexpr ui32 MaxRetries = 10000;
    static constexpr ui64 DefaultRowsToGenerate = 1000 * 1000;
    static constexpr ui64 DefaultRowsPerRequest = 10 * 1000;

    FLUENT_SETTING_DEFAULT(TDuration, OperationTimeout, TDuration::Seconds(5 * 60));
    FLUENT_SETTING_DEFAULT(TDuration, ClientTimeout, OperationTimeout_ + TDuration::Seconds(5));
    FLUENT_SETTING_DEFAULT(ui64, MaxInFlightRequests, 100);
    FLUENT_SETTING_DEFAULT(ui64, Threads, std::thread::hardware_concurrency());
    FLUENT_SETTING_DEFAULT(ui64, RowsToGenerate, DefaultRowsToGenerate);
    FLUENT_SETTING_DEFAULT(ui64, RowsPerRequest, DefaultRowsPerRequest);
};

class TGenerateClient {
public:
    explicit TGenerateClient(const TDriver& driver, const TClientCommand::TConfig& rootConfig);
    TGenerateClient(const TGenerateClient&) = delete;

    // Ingest generated data to the database table.
    //   dbPath: full path to the database table, including the database path
    //   settings: input data format and operational settings
    TStatus Generate(const TString& dbPath, const TGenerateSettings& settings = {});

private:
    std::shared_ptr<NOperation::TOperationClient> OperationClient;
    std::shared_ptr<NScheme::TSchemeClient> SchemeClient;
    std::shared_ptr<NTable::TTableClient> TableClient;

    NTable::TBulkUpsertSettings UpsertSettings;
    NTable::TRetryOperationSettings RetrySettings;

    using ProgressCallbackFunc = std::function<void (ui64, ui64)>;

    TStatus Upsert(const TString& dbPath, const std::vector<NYdb::NTable::TTableColumn>& tableColumns,
        const TGenerateSettings& settings, ProgressCallbackFunc& progressCallback);
    TAsyncStatus UpsertArrowBuffer(const TString& dbPath, const TString& buffer, const TString& strSchema);

    std::shared_ptr<arrow::RecordBatch> GenerateBatch(const std::vector<NYdb::NTable::TTableColumn>& tableColumns,
        std::shared_ptr<arrow::Schema>& schema, ui64 bytesPerRequest, ui64 numRows, ui64& seed, TString& errorString);
};

}
}
