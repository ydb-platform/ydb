#pragma once

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/public/api/protos/yq.pb.h> 

namespace NYq {

void AddClustersFromConnections(const THashMap<TString, YandexQuery::Connection>& connections,
    bool useBearerForYdb,
    const TString& objectStorageEndpoint,
    const TString& authToken,
    const THashMap<TString, TString>& accountIdSignatures,
    NYql::TGatewaysConfig& gatewaysConfig,
    THashMap<TString, TString>& clusters);

} //NYq
