#pragma once

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/protos/config.pb.h>


namespace NKikimr::NAudit {

using TAuditLogItemBuilder = TString(*)(TInstant, const TAuditLogParts&);

// Registration of a function for converting audit events to a string in a specified format
void RegisterAuditLogItemBuilder(NKikimrConfig::TAuditConfig::EFormat format, TAuditLogItemBuilder builder);

}
