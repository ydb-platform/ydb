#pragma once

#include <ydb/core/driver_lib/run/config.h>

#include <library/cpp/logger/backend.h>

namespace NKikimr {

TAutoPtr<TLogBackend> CreateLogBackendWithUnifiedAgent(
    const TKikimrRunConfig& runConfig,
    NMonitoring::TDynamicCounterPtr counters);

TAutoPtr<TLogBackend> CreateMeteringLogBackendWithUnifiedAgent(
    const TKikimrRunConfig& runConfig,
    NMonitoring::TDynamicCounterPtr counters);

TAutoPtr<TLogBackend> CreateAuditLogBackendWithUnifiedAgent(
    const TKikimrRunConfig& runConfig,
    NMonitoring::TDynamicCounterPtr counters);

} // NKikimr

