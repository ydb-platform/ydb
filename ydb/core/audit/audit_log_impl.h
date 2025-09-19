#pragma once
#include <ydb/core/audit/audit_log.h>

namespace NKikimr::NAudit {

void EscapeNonUtf8LogParts(TAuditLogParts& parts);

} // namespace NKikimr::NAudit
