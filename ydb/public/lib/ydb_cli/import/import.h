#pragma once

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

    FLUENT_SETTING_DEFAULT(NTable::EDataFormat, Format, NTable::EDataFormat::CSV);
    FLUENT_SETTING_DEFAULT(ui64, BytesPerRequest, 1_MB);
    FLUENT_SETTING_DEFAULT(ui64, FileBufferSize, 2_MB);
    FLUENT_SETTING_DEFAULT(ui64, MaxInFlightRequests, 100);
    FLUENT_SETTING_DEFAULT(ui32, SkipRows, 0);
    FLUENT_SETTING_DEFAULT(bool, Header, false);
    FLUENT_SETTING_DEFAULT(TString, Delimiter, DefaultDelimiter);
    FLUENT_SETTING_DEFAULT(TString, NullValue, "");
};

class TImportFileClient {
public:
    explicit TImportFileClient(const TDriver& driver);

    TStatus Import(const TString& fsPath, const TString& dbPath, const TImportFileSettings& settings = {});

private:
    std::shared_ptr<NOperation::TOperationClient> OperationClient;
    std::shared_ptr<NScheme::TSchemeClient> SchemeClient;
    std::shared_ptr<NTable::TTableClient> TableClient;

    TStatus UpsertCsv(const TString& dataFile, const TString& dbPath, const TImportFileSettings& settings);
    TAsyncStatus UpsertCsvBuffer(const TString& dbPath, const TString& csv, const TString& header,
                            const NTable::TBulkUpsertSettings& upsertSettings,
                            const NTable::TRetryOperationSettings& retrySettings);
};

}
}
