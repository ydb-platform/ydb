#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>

#include <ydb/public/sdk/cpp/client/ydb_types/s3_settings.h>

namespace NYdb {
namespace NImport {

/// Common
enum class EImportProgress {
    Unspecified = 0,
    Preparing = 1,
    TransferData = 2,
    BuildIndexes = 3,
    Done = 4,
    Cancellation = 5,
    Cancelled = 6,

    Unknown = Max<int>(),
};

struct TImportItemProgress {
    ui32 PartsTotal;
    ui32 PartsCompleted;
    TInstant StartTime;
    TInstant EndTime;
};

/// S3
struct TImportFromS3Settings : public TOperationRequestSettings<TImportFromS3Settings>,
                               public TS3Settings<TImportFromS3Settings> {
    using TSelf = TImportFromS3Settings;

    struct TItem {
        TString Src;
        TString Dst;
    };

    FLUENT_SETTING_VECTOR(TItem, Item);
    FLUENT_SETTING_OPTIONAL(TString, Description);
    FLUENT_SETTING_OPTIONAL(ui32, NumberOfRetries);
};

class TImportFromS3Response : public TOperation {
public:
    struct TMetadata {
        TImportFromS3Settings Settings;
        EImportProgress Progress;
        TVector<TImportItemProgress> ItemsProgress;
    };

public:
    using TOperation::TOperation;
    TImportFromS3Response(TStatus&& status, Ydb::Operations::Operation&& operation);

    const TMetadata& Metadata() const;

private:
    TMetadata Metadata_;
};

/// Data
struct TImportYdbDumpDataSettings : public TOperationRequestSettings<TImportYdbDumpDataSettings> {
    using TSelf = TImportYdbDumpDataSettings;

    FLUENT_SETTING_VECTOR(TString, Columns);

    using TOperationRequestSettings::TOperationRequestSettings;
};

class TImportDataResult : public TStatus {
public:
    explicit TImportDataResult(TStatus&& status);
};

using TAsyncImportDataResult = NThreading::TFuture<TImportDataResult>;

class TImportClient {
    class TImpl;

public:
    TImportClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    NThreading::TFuture<TImportFromS3Response> ImportFromS3(const TImportFromS3Settings& settings);

    // ydb dump format
    TAsyncImportDataResult ImportData(const TString& table, TString&& data, const TImportYdbDumpDataSettings& settings);
    TAsyncImportDataResult ImportData(const TString& table, const TString& data, const TImportYdbDumpDataSettings& settings);

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NImport
} // namespace NYdb

template<>
inline void Out<NYdb::NImport::TImportFromS3Response>(IOutputStream& o, const NYdb::NImport::TImportFromS3Response& x) {
    return x.Out(o);
}
