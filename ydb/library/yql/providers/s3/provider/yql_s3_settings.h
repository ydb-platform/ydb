#pragma once

#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>

#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/config/yql_setting.h>

namespace NYql {

class IS3ConfSettingWithDisableCheck {
public:
    virtual ~IS3ConfSettingWithDisableCheck() = default;

    virtual void CheckDisabled(TExprContext& ctx) = 0;
};

template <typename TType>
class IS3ConfSettingWithDisable : public IS3ConfSettingWithDisableCheck {
public:
    virtual void Disable(TType value, const TString& reason) = 0;
};

template <typename TType, TType Default>
class TS3ConfSettingDefault final : NCommon::TConfSetting<TType, NCommon::EConfSettingType::Static>, public IS3ConfSettingWithDisable<TType> {
    using TBase = NCommon::TConfSetting<TType, NCommon::EConfSettingType::Static>;

    friend struct TS3Configuration;

public:
    TType GetOrDefault() const {
        return DisabledValue.value_or(TBase::Get().GetOrElse(Default));
    }

protected:
    void Disable(TType value, const TString& reason) final {
        DisabledValue = value;
        DisabledReason = reason;
    }

    void CheckDisabled(TExprContext& ctx) final {
        if (DisabledValue && TBase::Get() && !DisableReported) {
            ctx.AddWarning(TIssue(DisabledReason).SetCode(DEFAULT_ERROR, TSeverityIds::S_WARNING));
            DisableReported = true;
        }
    }

private:
    std::optional<TType> DisabledValue;
    TString DisabledReason;
    bool DisableReported = false;
};

struct TS3Settings {
private:
    template <typename TType>
    using TS3ConfSettingOptional = NCommon::TConfSetting<TType, NCommon::EConfSettingType::Static>; // Default value selected in runtime

public:
    using TConstPtr = std::shared_ptr<const TS3Settings>;

    TS3ConfSettingDefault<bool, true> SourceCoroActor;
    TS3ConfSettingDefault<ui64, 50_MB> MaxOutputObjectSize;           // Maximum size of written to S3 file
    TS3ConfSettingDefault<ui64, 4096> UniqueKeysCountLimit;
    TS3ConfSettingDefault<ui64, 1_MB> BlockSizeMemoryLimit;           // Maximum size of single part for S3 writing
    TS3ConfSettingDefault<ui64, 10_MB> SerializeMemoryLimit;          // Total serialization memory limit for all current blocks for all partition keys. Reachable in case of many but small partitions
    TS3ConfSettingDefault<ui64, 1_GB> InFlightMemoryLimit;            // Maximum memory used by one sink
    TS3ConfSettingDefault<ui64, 10'000> JsonListSizeLimit;            // Limit of elements count in json list written to S3 file. Default: 10'000. Max: 100'000
    TS3ConfSettingDefault<ui64, 0> ArrowParallelRowGroupCount;        // Number of parquet row groups to read in parallel, min == 1
    TS3ConfSettingDefault<bool, true> ArrowRowGroupReordering;        // Allow to push rows from file in any order, default false, but usually it is OK
    TS3ConfSettingDefault<ui64, 0> ParallelDownloadCount;             // Number of files to read in parallel, min == 1
    TS3ConfSettingDefault<bool, true> UseBlocksSource;                // Deprecated and has to be removed after config cleanup
    TS3ConfSettingOptional<bool> UseBlocksSink;                       // Automatically enabled by default if all types are supported
    TS3ConfSettingDefault<bool, false> AtomicUploadCommit;            // Commit each file independently, w/o transaction semantic over all files
    TS3ConfSettingOptional<bool> UseConcurrentDirectoryLister;        // By default TS3GatewayConfig::AllowConcurrentListings
    TS3ConfSettingOptional<ui64> MaxDiscoveryFilesPerDirectory;       // By default TS3GatewayConfig::MaxListingResultSizePerPartition
    TS3ConfSettingDefault<bool, false> UseRuntimeListing;             // Enables runtime listing
    TS3ConfSettingDefault<ui64, 1000'000> FileQueueBatchSizeLimit;    // Limits total size of files in one PathBatch from FileQueue
    TS3ConfSettingDefault<ui64, 1000> FileQueueBatchObjectCountLimit; // Limits count of files in one PathBatch from FileQueue
    TS3ConfSettingOptional<ui64> FileQueuePrefetchSize;
    TS3ConfSettingDefault<bool, false> AsyncDecoding;                 // Parse and decode input data at separate mailbox/thread of TaskRunner
    TS3ConfSettingDefault<bool, false> UsePredicatePushdown;
    TS3ConfSettingDefault<bool, false> AsyncDecompressing;            // Decompression and parsing input data in different mailbox/thread
};

struct TS3ClusterSettings {
    TString Url;
};

struct TS3Configuration : public TS3Settings, public NCommon::TSettingDispatcher {
    using TPtr = TIntrusivePtr<TS3Configuration>;
    using TSetupper = std::function<void(TS3Configuration& configuration)>;

    TS3Configuration();
    TS3Configuration(const TS3Configuration&) = delete;

    void Init(const TS3GatewayConfig& config, TIntrusivePtr<TTypeAnnotationContext> typeCtx);

    bool HasCluster(TStringBuf cluster) const;

    void CheckDisabledPragmas(TExprContext& ctx) const;

    template <typename TType>
    void DisablePragma(IS3ConfSettingWithDisable<TType>& pragma, TType value, const TString& reason) {
        pragma.Disable(value, reason);
        DisabledPragmas.emplace_back(&pragma);
    }

    TS3Settings::TConstPtr Snapshot() const;
    THashMap<TString, TString> Tokens;
    std::unordered_map<TString, TS3ClusterSettings> Clusters;

    ui64 FileSizeLimit = 0;
    ui64 BlockFileSizeLimit = 0;
    std::unordered_map<TString, ui64> FormatSizeLimits;
    ui64 MaxFilesPerQuery = 0;
    ui64 MaxDiscoveryFilesPerQuery = 0;
    ui64 MaxDirectoriesAndFilesPerQuery = 0;
    ui64 MinDesiredDirectoriesOfFilesPerQuery = 0;
    ui64 MaxInflightListsPerQuery = 0;
    ui64 ListingCallbackThreadCount = 0;
    ui64 ListingCallbackPerThreadQueueSize = 0;
    ui64 RegexpCacheSize = 0;
    bool AllowLocalFiles = false;
    bool AllowConcurrentListings = false;
    ui64 GeneratorPathsLimit = 0;
    bool WriteThroughDqIntegration = false;
    ui64 MaxListingResultSizePerPhysicalPartition;
    bool AllowAtomicUploadCommit = true;
    NDq::TS3ReadActorFactoryConfig S3ReadActorFactoryConfig;

private:
    std::vector<IS3ConfSettingWithDisableCheck*> DisabledPragmas;
};

} // namespace NYql
