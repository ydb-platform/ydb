#include "gateways_utils.h"
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {
void GetClusterMappingFromGateways(const NYql::TGatewaysConfig& gateways, THashMap<TString, TString>& clusterMapping) {
    clusterMapping.clear();
    clusterMapping["pg_catalog"] = PgProviderName;
    clusterMapping["information_schema"] = PgProviderName;
    if (gateways.HasYt()) {
        AddClusters(gateways.GetYt().GetClusterMapping(),
            TString{YtProviderName},
            &clusterMapping);
    }
    if (gateways.HasClickHouse()) {
        AddClusters(gateways.GetClickHouse().GetClusterMapping(),
            TString{ClickHouseProviderName},
            &clusterMapping);
    }
    if (gateways.HasS3()) {
        AddClusters(gateways.GetS3().GetClusterMapping(),
            TString{S3ProviderName},
            &clusterMapping);
    }
    if (gateways.HasYdb() && !gateways.HasKikimr()) {
        AddClusters(gateways.GetYdb().GetClusterMapping(),
            TString{YdbProviderName},
            &clusterMapping);
    }
}

THashSet<TString> ExtractSqlFlags(const TGatewaysConfig& gateways) {
    THashSet<TString> flags;
    if (gateways.HasSqlCore()) {
        flags.insert(gateways.GetSqlCore().GetTranslationFlags().cbegin(), gateways.GetSqlCore().GetTranslationFlags().cend());
    }
    return flags;
}
}
