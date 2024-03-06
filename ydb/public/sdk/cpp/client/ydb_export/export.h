#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>

#include <ydb/public/sdk/cpp/client/ydb_types/s3_settings.h>

namespace NYdb {
namespace NExport {

/// Common
enum class EExportProgress {
    Unspecified = 0,
    Preparing = 1,
    TransferData = 2,
    Done = 3,
    Cancellation = 4,
    Cancelled = 5,

    Unknown = Max<int>(),
};

struct TExportItemProgress {
    ui32 PartsTotal;
    ui32 PartsCompleted;
    TInstant StartTime;
    TInstant EndTime;
};

/// YT
struct TExportToYtSettings : public TOperationRequestSettings<TExportToYtSettings> {
    struct TItem {
        TString Src;
        TString Dst;
    };

    FLUENT_SETTING(TString, Host);
    FLUENT_SETTING_OPTIONAL(ui16, Port);
    FLUENT_SETTING(TString, Token);
    FLUENT_SETTING_VECTOR(TItem, Item);
    FLUENT_SETTING_OPTIONAL(TString, Description);
    FLUENT_SETTING_OPTIONAL(ui32, NumberOfRetries);
    FLUENT_SETTING_DEFAULT(bool, UseTypeV3, false);
};

class TExportToYtResponse : public TOperation {
public:
    struct TMetadata {
        TExportToYtSettings Settings;
        EExportProgress Progress;
        TVector<TExportItemProgress> ItemsProgress;
    };

public:
    using TOperation::TOperation;
    TExportToYtResponse(TStatus&& status, Ydb::Operations::Operation&& operation);

    const TMetadata& Metadata() const;

private:
    TMetadata Metadata_;
};

/// S3
struct TExportToS3Settings : public TOperationRequestSettings<TExportToS3Settings>,
                             public TS3Settings<TExportToS3Settings> {
    using TSelf = TExportToS3Settings;

    enum class EStorageClass {
        NOT_SET = 0,
        STANDARD = 1,
        REDUCED_REDUNDANCY = 2,
        STANDARD_IA = 3,
        ONEZONE_IA = 4,
        INTELLIGENT_TIERING = 5,
        GLACIER = 6,
        DEEP_ARCHIVE = 7,
        OUTPOSTS = 8,

        UNKNOWN = Max<int>(),
    };

    struct TItem {
        TString Src;
        TString Dst;
    };

    FLUENT_SETTING_DEFAULT(EStorageClass, StorageClass, EStorageClass::NOT_SET);
    FLUENT_SETTING_VECTOR(TItem, Item);
    FLUENT_SETTING_OPTIONAL(TString, Description);
    FLUENT_SETTING_OPTIONAL(ui32, NumberOfRetries);
    FLUENT_SETTING_OPTIONAL(TString, Compression);
};

class TExportToS3Response : public TOperation {
public:
    struct TMetadata {
        TExportToS3Settings Settings;
        EExportProgress Progress;
        TVector<TExportItemProgress> ItemsProgress;
    };

public:
    using TOperation::TOperation;
    TExportToS3Response(TStatus&& status, Ydb::Operations::Operation&& operation);

    const TMetadata& Metadata() const;

private:
    TMetadata Metadata_;
};

class TExportClient {
    class TImpl;

public:
    TExportClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    NThreading::TFuture<TExportToYtResponse> ExportToYt(const TExportToYtSettings& settings);
    NThreading::TFuture<TExportToS3Response> ExportToS3(const TExportToS3Settings& settings);

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NExport
} // namespace NYdb
