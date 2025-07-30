#pragma once

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

namespace NYql::NSo {

NSo::NProto::ESolomonClusterType MapClusterType(TSolomonClusterConfig::ESolomonClusterType clusterType);

std::map<TString, TString> ExtractSelectorValues(const NSo::NProto::TDqSolomonSource& source, const TString& selectors);

NProto::TDqSolomonSource FillSolomonSource(const TSolomonClusterConfig* config, const TString& project);

} // namespace NYql::NSo
