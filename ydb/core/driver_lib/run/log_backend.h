#pragma once

#include "config.h"
#include <library/cpp/logger/backend.h>

namespace NKikimr {

// Interface for LogBackend creation
class ILogBackendFactory {
public:
    virtual TAutoPtr<TLogBackend> CreateLogBackend(
            const TKikimrRunConfig& runConfig,
            ::NMonitoring::TDynamicCounterPtr counters) = 0;

    virtual ~ILogBackendFactory() {}
};

// Default implementation if ILogBackendFactory
class TLogBackendFactory : public ILogBackendFactory {
protected:
    TAutoPtr<TLogBackend> CreateLogBackendFromLogConfig(const TKikimrRunConfig& runConfig);

public:
    virtual TAutoPtr<TLogBackend> CreateLogBackend(
            const TKikimrRunConfig& runConfig,
            ::NMonitoring::TDynamicCounterPtr counters) override;
};

} // NKikimr
