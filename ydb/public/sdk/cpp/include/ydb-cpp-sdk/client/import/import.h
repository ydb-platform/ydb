#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/s3_settings.h>

namespace Ydb::Import {
class ListObjectsInS3ExportResult;
}

namespace NYdb::inline Dev {

class TProtoAccessor;

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
    CreateChangefeeds = 7,

    Unknown = std::numeric_limits<int>::max(),
};

struct TImportItemProgress {
    uint32_t PartsTotal;
    uint32_t PartsCompleted;
    TInstant StartTime;
    TInstant EndTime;
};

/// S3
struct TImportFromS3Settings : public TOperationRequestSettings<TImportFromS3Settings>,
                               public TS3Settings<TImportFromS3Settings> {
    using TSelf = TImportFromS3Settings;

    struct TItem {
        // Source prefix.
        // S3 prefix for item
        std::string Src;

        // Destination path.
        // database path where to import data
        std::string Dst;

        // Source path.
        // if the export contains the database objects list, you may specify the database object name,
        // and the S3 prefix will be looked up in the database objects list by the import procedure
        std::string SrcPath = {};
    };

    FLUENT_SETTING_VECTOR(TItem, Item);
    FLUENT_SETTING_OPTIONAL(std::string, Description);
    FLUENT_SETTING_OPTIONAL(uint32_t, NumberOfRetries);
    FLUENT_SETTING_OPTIONAL(bool, NoACL);
    FLUENT_SETTING_OPTIONAL(bool, SkipChecksumValidation);
    FLUENT_SETTING_OPTIONAL(std::string, SourcePrefix);
    FLUENT_SETTING_OPTIONAL(std::string, DestinationPath);
    FLUENT_SETTING_OPTIONAL(std::string, SymmetricKey);
};

class TImportFromS3Response : public TOperation {
public:
    struct TMetadata {
        TImportFromS3Settings Settings;
        EImportProgress Progress;
        std::vector<TImportItemProgress> ItemsProgress;
    };

public:
    using TOperation::TOperation;
    TImportFromS3Response(TStatus&& status, Ydb::Operations::Operation&& operation);

    const TMetadata& Metadata() const;

private:
    TMetadata Metadata_;
};

using TAsyncImportFromS3Response = NThreading::TFuture<TImportFromS3Response>;

struct TListObjectsInS3ExportSettings : public TOperationRequestSettings<TListObjectsInS3ExportSettings>,
                                        public TS3Settings<TListObjectsInS3ExportSettings> {
    using TSelf = TListObjectsInS3ExportSettings;

    struct TItem {
        // Database object path.
        std::string Path = {};
    };

    FLUENT_SETTING_VECTOR(TItem, Item);
    FLUENT_SETTING_OPTIONAL(uint32_t, NumberOfRetries);
    FLUENT_SETTING_OPTIONAL(std::string, Prefix);
    FLUENT_SETTING_OPTIONAL(std::string, SymmetricKey);
};

class TListObjectsInS3ExportResult : public TStatus {
    friend class NYdb::TProtoAccessor;

public:
    struct TItem {
        // S3 object prefix
        std::string Prefix;

        // Database object path
        std::string Path;

        void Out(IOutputStream& out) const;
    };

    TListObjectsInS3ExportResult(TStatus&& status, const Ydb::Import::ListObjectsInS3ExportResult& proto);
    TListObjectsInS3ExportResult(TListObjectsInS3ExportResult&&);
    TListObjectsInS3ExportResult(const TListObjectsInS3ExportResult&);
    ~TListObjectsInS3ExportResult();

    TListObjectsInS3ExportResult& operator=(TListObjectsInS3ExportResult&&);
    TListObjectsInS3ExportResult& operator=(const TListObjectsInS3ExportResult&);

    const std::vector<TItem>& GetItems() const;
    const std::string& NextPageToken() const { return NextPageToken_; }

    void Out(IOutputStream& out) const;

private:
    const Ydb::Import::ListObjectsInS3ExportResult& GetProto() const;

private:
    std::vector<TItem> Items_;
    std::string NextPageToken_;
    std::unique_ptr<Ydb::Import::ListObjectsInS3ExportResult> Proto_;
};

using TAsyncListObjectsInS3ExportResult = NThreading::TFuture<TListObjectsInS3ExportResult>;

/// Data
struct TImportYdbDumpDataSettings : public TOperationRequestSettings<TImportYdbDumpDataSettings> {
    using TSelf = TImportYdbDumpDataSettings;

    FLUENT_SETTING_VECTOR(std::string, Columns);

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

    TAsyncImportFromS3Response ImportFromS3(const TImportFromS3Settings& settings);

    TAsyncListObjectsInS3ExportResult ListObjectsInS3Export(const TListObjectsInS3ExportSettings& settings, std::int64_t pageSize = 0, const std::string& pageToken = {});

    // ydb dump format
    TAsyncImportDataResult ImportData(const std::string& table, std::string&& data, const TImportYdbDumpDataSettings& settings);
    TAsyncImportDataResult ImportData(const std::string& table, const std::string& data, const TImportYdbDumpDataSettings& settings);

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NImport
} // namespace NYdb
