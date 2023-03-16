#pragma once

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

namespace NFq {

void AddSystemClusters(NYql::TGatewaysConfig& gatewaysConfig, THashMap<TString, TString>& clusters, const TString& authToken);

} //NFq
