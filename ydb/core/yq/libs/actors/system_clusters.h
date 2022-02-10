#pragma once

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

namespace NYq {

void AddSystemClusters(NYql::TGatewaysConfig& gatewaysConfig, THashMap<TString, TString>& clusters, const TString& authToken);

} //NYq
