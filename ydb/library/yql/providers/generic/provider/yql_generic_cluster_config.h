#pragma once

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <util/generic/hash.h>

namespace NYql {
    NYql::TGenericClusterConfig GenericClusterConfigFromProperties(
        const TString& clusterName,
        const THashMap<TString, TString>& properties);
}
