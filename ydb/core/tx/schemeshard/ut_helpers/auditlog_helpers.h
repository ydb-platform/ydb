#pragma once

#include <string>
#include <vector>

#include <ydb/core/audit/audit_log_service.h>

namespace NSchemeShardUT_Private {

using namespace NKikimr;

using TMemoryLogBackend = NKikimr::Tests::TMemoryLogBackend;
using NKikimr::Tests::CreateTestAuditLogBackends;

std::string FindAuditLine(const std::vector<std::string>& auditLines, const std::string& substr);

}  // namespace NSchemeShardUT_Private
