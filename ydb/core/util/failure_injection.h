#pragma once

#include "defs.h"
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/http/audit/auditable_actions.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr {

    NActors::IActor *CreateFailureInjectionActor(const NKikimrConfig::TFailureInjectionConfig& config, const NKikimr::TAppData& appData);
    ::NHttp::NAudit::EAuditableAction FailureInjectionAuditResolver(const NMonitoring::IMonHttpRequest& request);

} // NKikimr
