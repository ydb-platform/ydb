#pragma once
#include <ydb/core/driver_lib/run/log_backend.h>

namespace NKikimr {

class  TAuditLogBackendFactory : public TLogBackendFactory {
public:
    virtual TAutoPtr<TLogBackend> CreateLogBackend(
            const TKikimrRunConfig& runConfig,
            NMonitoring::TDynamicCounterPtr counters) override;
};

} // NKikimr
