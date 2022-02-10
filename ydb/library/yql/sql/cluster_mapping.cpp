#include "cluster_mapping.h"
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

using namespace NYql;

namespace NSQLTranslation {
TClusterMapping::TClusterMapping(const THashMap<TString, TString>& mapping) {
    for (const auto& p : mapping) {
        if (p.second == KikimrProviderName) {
            CaseSensitiveClusters.emplace(p);
            continue;
        }

        TString clusterLowerCase = to_lower(p.first);
        CaseInsensitiveClusters.emplace(clusterLowerCase, p.second);
    }
}

TMaybe<TString> TClusterMapping::GetClusterProvider(const TString& cluster, TString& normalizedClusterName) const {
    auto providerPtr1 = CaseSensitiveClusters.FindPtr(cluster);
    if (providerPtr1) {
        normalizedClusterName = cluster;
        return *providerPtr1;
    }

    TString clusterLowerCase = to_lower(cluster);
    auto providerPtr2 = CaseInsensitiveClusters.FindPtr(clusterLowerCase);
    if (providerPtr2) {
        normalizedClusterName = clusterLowerCase;
        return *providerPtr2;
    }
    return Nothing();
}
}
