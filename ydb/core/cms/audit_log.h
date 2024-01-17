#pragma once

#include "defs.h"

#include <ydb/library/actors/core/mon.h>

namespace NKikimr::NCms {

void AuditLog(const TString& component, const TString& message, const TActorContext& ctx);
void AuditLog(const TString& component, const IEventBase* request, const IEventBase* response, const TActorContext& ctx);

// for http request/response
void AuditLog(const TString& component, NMon::TEvHttpInfo::TPtr& request, const TString& response, const TActorContext& ctx);

} // namespace NKikimr::NCms
