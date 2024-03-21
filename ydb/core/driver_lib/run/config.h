#pragma once

#include "service_mask.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/base/event_filter.h>
#include <ydb/core/config/init/init.h>
#include <ydb/core/driver_lib/cli_config_base/config_base.h>

#include <util/generic/hash.h>

#include <google/protobuf/text_format.h>

namespace NKikimr {

struct TKikimrRunConfig {
    NKikimrConfig::TAppConfig& AppConfig;
    ui32 NodeId;
    TKikimrScopeId ScopeId;

    TString PathToConfigCacheFile;

    TString TenantName;
    TBasicKikimrServicesMask ServicesMask;

    TString ClusterName;

    NConfig::TConfigsDispatcherInitInfo ConfigsDispatcherInitInfo;

    TKikimrRunConfig(NKikimrConfig::TAppConfig& appConfig,
                     ui32 nodeId = 0, const TKikimrScopeId& scopeId = {});
};

}
