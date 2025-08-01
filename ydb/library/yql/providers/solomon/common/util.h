#pragma once

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

namespace NYql::NSo {

TMaybe<TString> ParseSelectorValues(const TString& selectors, std::map<TString, TString>& result);
TMaybe<TString> BuildSelectorValues(const NSo::NProto::TDqSolomonSource& source, const TString& selectors, std::map<TString, TString>& result);
    
NSo::NProto::ESolomonClusterType MapClusterType(TSolomonClusterConfig::ESolomonClusterType clusterType);

NProto::TDqSolomonSource FillSolomonSource(const TSolomonClusterConfig* config, const TString& project);

} // namespace NYql::NSo
