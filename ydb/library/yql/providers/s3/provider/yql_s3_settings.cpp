#include "yql_s3_settings.h"
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <util/generic/size_literals.h>

namespace NYql {

using namespace NCommon;

TS3Configuration::TS3Configuration()
{
    REGISTER_SETTING(*this, SourceCoroActor);
    REGISTER_SETTING(*this, MaxOutputObjectSize);
    REGISTER_SETTING(*this, UniqueKeysCountLimit);
    REGISTER_SETTING(*this, BlockSizeMemoryLimit);
    REGISTER_SETTING(*this, SerializeMemoryLimit);
    REGISTER_SETTING(*this, InFlightMemoryLimit);
    REGISTER_SETTING(*this, JsonListSizeLimit).Upper(100'000);
    REGISTER_SETTING(*this, ArrowParallelRowGroupCount).Lower(1);
    REGISTER_SETTING(*this, ArrowRowGroupReordering);
    REGISTER_SETTING(*this, ParallelDownloadCount);
    REGISTER_SETTING(*this, UseBlocksSource);
    REGISTER_SETTING(*this, AtomicUploadCommit);
    REGISTER_SETTING(*this, UseConcurrentDirectoryLister);
    REGISTER_SETTING(*this, MaxDiscoveryFilesPerDirectory).Lower(1);
    REGISTER_SETTING(*this, UseRuntimeListing);
    REGISTER_SETTING(*this, FileQueueBatchSizeLimit);
    REGISTER_SETTING(*this, FileQueueBatchObjectCountLimit);
    REGISTER_SETTING(*this, FileQueuePrefetchSize);
    REGISTER_SETTING(*this, AsyncDecoding);
    REGISTER_SETTING(*this, UsePredicatePushdown);
    REGISTER_SETTING(*this, AsyncDecompressing);
}

TS3Settings::TConstPtr TS3Configuration::Snapshot() const {
    return std::make_shared<const TS3Settings>(*this);
}

bool TS3Configuration::HasCluster(TStringBuf cluster) const {
    return ValidClusters.contains(cluster);
}

void TS3Configuration::Init(const TS3GatewayConfig& config, TIntrusivePtr<TTypeAnnotationContext> typeCtx)
{
    for (auto& formatSizeLimit: config.GetFormatSizeLimit()) {
        if (formatSizeLimit.GetName()) { // ignore unnamed limits
            FormatSizeLimits.emplace(formatSizeLimit.GetName(), formatSizeLimit.GetFileSizeLimit());
        }
    }
    S3ReadActorFactoryConfig = NDq::CreateReadActorFactoryConfig(config);
    FileSizeLimit = config.HasFileSizeLimit() ? config.GetFileSizeLimit() : 2_GB;
    BlockFileSizeLimit = config.HasBlockFileSizeLimit() ? config.GetBlockFileSizeLimit() : 50_GB;
    MaxFilesPerQuery = config.HasMaxFilesPerQuery() ? config.GetMaxFilesPerQuery() : 7000;
    MaxDiscoveryFilesPerQuery = config.HasMaxDiscoveryFilesPerQuery()
                                    ? config.GetMaxDiscoveryFilesPerQuery()
                                    : 9000;
    MaxDirectoriesAndFilesPerQuery = config.HasMaxDirectoriesAndFilesPerQuery()
                                         ? config.GetMaxDirectoriesAndFilesPerQuery()
                                         : 9000;
    MinDesiredDirectoriesOfFilesPerQuery =
        config.HasMinDesiredDirectoriesOfFilesPerQuery()
            ? config.GetMinDesiredDirectoriesOfFilesPerQuery()
            : 100;
    MaxReadSizePerQuery =
        config.HasMaxReadSizePerQuery() ? config.GetMaxReadSizePerQuery() : 4_GB;
    MaxInflightListsPerQuery =
        config.HasMaxInflightListsPerQuery() ? config.GetMaxInflightListsPerQuery() : 1;
    ListingCallbackThreadCount = config.HasListingCallbackThreadCount()
                                     ? config.GetListingCallbackThreadCount()
                                     : 1;
    ListingCallbackPerThreadQueueSize = config.HasListingCallbackPerThreadQueueSize()
                                            ? config.GetListingCallbackPerThreadQueueSize()
                                            : 100;
    RegexpCacheSize = config.HasRegexpCacheSize() ? config.GetRegexpCacheSize() : 100;
    AllowConcurrentListings =
        config.HasAllowConcurrentListings() ? config.GetAllowConcurrentListings() : false;
    GeneratorPathsLimit =
        config.HasGeneratorPathsLimit() ? config.GetGeneratorPathsLimit() : 50'000;
    MaxListingResultSizePerPhysicalPartition =
        config.HasMaxListingResultSizePerPartition()
            ? config.GetMaxListingResultSizePerPartition()
            : 1'000;

    TVector<TString> clusters(Reserve(config.ClusterMappingSize()));
    for (auto& cluster: config.GetClusterMapping()) {
        clusters.push_back(cluster.GetName());
    }

    this->SetValidClusters(clusters);
    this->Dispatch(config.GetDefaultSettings());

    for (const auto& cluster: config.GetClusterMapping()) {
        this->Dispatch(cluster.GetName(), cluster.GetSettings());
        auto& settings = Clusters[cluster.GetName()];
        settings.Url = cluster.GetUrl();
        TString authToken;
        if (const auto& token = cluster.GetToken()) {
            authToken = typeCtx->Credentials->FindCredentialContent("cluster:default_" + cluster.GetName(), "", token);
        }
        Tokens[cluster.GetName()] = ComposeStructuredTokenJsonForServiceAccount(cluster.GetServiceAccountId(), cluster.GetServiceAccountIdSignature(), authToken);
    }
    this->FreezeDefaults();
}

} // NYql
