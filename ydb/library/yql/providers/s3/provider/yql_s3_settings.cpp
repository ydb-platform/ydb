#include "yql_s3_settings.h"
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <util/generic/size_literals.h>

namespace NYql {

using namespace NCommon;

TS3Configuration::TS3Configuration()
{
    REGISTER_SETTING(*this, SourceActor);
}

TS3Settings::TConstPtr TS3Configuration::Snapshot() const {
    return std::make_shared<const TS3Settings>(*this);
}

bool TS3Configuration::HasCluster(TStringBuf cluster) const {
    return ValidClusters.contains(cluster);
}

void TS3Configuration::Init(const TS3GatewayConfig& config, TIntrusivePtr<TTypeAnnotationContext> typeCtx) 
{
    FileSizeLimit = config.HasFileSizeLimit() ? config.GetFileSizeLimit() : 2_GB;
    MaxFilesPerQuery = config.HasMaxFilesPerQuery() ? config.GetMaxFilesPerQuery() : 7000;
    MaxReadSizePerQuery = config.HasMaxReadSizePerQuery() ? config.GetMaxReadSizePerQuery() : 4_GB;

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
            authToken = typeCtx->FindCredentialContent("cluster:default_" + cluster.GetName(), "", token);
        } 
        Tokens[cluster.GetName()] = ComposeStructuredTokenJsonForServiceAccount(cluster.GetServiceAccountId(), cluster.GetServiceAccountIdSignature(), authToken); 
    }
    this->FreezeDefaults();
}

} // NYql
