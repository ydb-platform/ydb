#pragma once

#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

namespace NYql {

struct TS3Settings {
    using TConstPtr = std::shared_ptr<const TS3Settings>;

    NCommon::TConfSetting<bool, false> SourceCoroActor;
    NCommon::TConfSetting<ui64, false> MaxOutputObjectSize;
    NCommon::TConfSetting<ui64, false> UniqueKeysCountLimit;
    NCommon::TConfSetting<ui64, false> BlockSizeMemoryLimit;
    NCommon::TConfSetting<ui64, false> SerializeMemoryLimit; // Total serialization memory limit for all current blocks for all patition keys. Reachable in case of many but small partitions.
    NCommon::TConfSetting<ui64, false> InFlightMemoryLimit; // Maximum memory used by one sink.
    NCommon::TConfSetting<ui64, false> JsonListSizeLimit; // Limit of elements count in json list written to S3 file. Default: 10'000. Max: 100'000.
    NCommon::TConfSetting<ui64, false> ArrowParallelRowGroupCount; // Number of parquet row groups to read in parallel, min == 1
    NCommon::TConfSetting<bool, false> ArrowRowGroupReordering;    // Allow to push rows from file in any order, default false, but usually it is OK
    NCommon::TConfSetting<ui64, false> ParallelDownloadCount;      // Number of files to read in parallel, min == 1
    NCommon::TConfSetting<bool, false> UseBlocksSource;            // Use blocks source (if exists) for scalar MKQL mode
    NCommon::TConfSetting<bool, false> AtomicUploadCommit;         // Commit each file independently, w/o transaction semantic over all files
    NCommon::TConfSetting<bool, false> UseConcurrentDirectoryLister;
};

struct TS3ClusterSettings {
    TString Url, Token;
};

struct TS3Configuration : public TS3Settings, public NCommon::TSettingDispatcher {
    using TPtr = TIntrusivePtr<TS3Configuration>;

    TS3Configuration();
    TS3Configuration(const TS3Configuration&) = delete;

    void Init(const TS3GatewayConfig& config, TIntrusivePtr<TTypeAnnotationContext> typeCtx);

    bool HasCluster(TStringBuf cluster) const;

    TS3Settings::TConstPtr Snapshot() const;
    THashMap<TString, TString> Tokens;
    std::unordered_map<TString, TS3ClusterSettings> Clusters;

    ui64 FileSizeLimit;
    ui64 BlockFileSizeLimit;
    std::unordered_map<TString, ui64> FormatSizeLimits;
    ui64 MaxFilesPerQuery;
    ui64 MaxDiscoveryFilesPerQuery;
    ui64 MaxDirectoriesAndFilesPerQuery;
    ui64 MinDesiredDirectoriesOfFilesPerQuery;
    ui64 MaxReadSizePerQuery;
    ui64 MaxInflightListsPerQuery;
    ui64 ListingCallbackThreadCount;
    ui64 ListingCallbackPerThreadQueueSize;
    ui64 RegexpCacheSize;
    bool AllowLocalFiles;
    bool AllowConcurrentListings;
    ui64 GeneratorPathsLimit;
};

} // NYql
