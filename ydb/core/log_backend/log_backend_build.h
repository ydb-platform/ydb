#pragma once

#include <ydb/core/base/counters.h>
#include <ydb/core/protos/config.pb.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/unified_agent_client/backend.h>
#include <util/generic/ptr.h>

namespace NKikimr {

class TLogBackendBuildHelper {
public:
    static TAutoPtr<TLogBackend> CreateLogBackendFromLogConfig(
            const NKikimrConfig::TLogConfig& logConfig,
            const TString& defaultIdent = "KIKIMR");
    static TAutoPtr<TLogBackend> CreateLogBackendFromUAClientConfig(
            const NKikimrConfig::TUAClientConfig& uaClientConfig,
            NMonitoring::TDynamicCounterPtr uaCounters,
            const TString& logName,
            const TString& nodeType,
            const TString& tenant,
            const TString& clusterName);
};
}
