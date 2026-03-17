#pragma once

#include <ydb/core/driver_lib/run/config.h>
#include <ydb/core/protos/audit.pb.h>

#include <library/cpp/logger/backend.h>

namespace NKikimr {

TAutoPtr<TLogBackend> CreateLogBackendWithUnifiedAgent(
    const TKikimrRunConfig& runConfig,
    NMonitoring::TDynamicCounterPtr counters);

TAutoPtr<TLogBackend> CreateMeteringLogBackendWithUnifiedAgent(
    const TKikimrRunConfig& runConfig,
    NMonitoring::TDynamicCounterPtr counters);

TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>> CreateAuditLogBackends(
    const TKikimrRunConfig& runConfig,
    NMonitoring::TDynamicCounterPtr counters);

void AddAuditConfigLogBackends(
    const NKikimrConfig::TAuditConfig& auditConfig,
    TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>>& logBackends,
    const TKikimrRunConfig& runConfig,
    NMonitoring::TDynamicCounterPtr counters);

} // NKikimr

