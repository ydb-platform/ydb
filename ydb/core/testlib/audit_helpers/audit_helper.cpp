#include "audit_helper.h"

namespace NKikimr {
namespace Tests {

NAudit::TAuditLogBackends CreateTestAuditLogBackends(std::vector<std::string>& lineBuffer) {
    NAudit::TAuditLogBackends logBackends;
    logBackends[NKikimrConfig::TAuditConfig::TXT].emplace_back(new TMemoryLogBackend(lineBuffer));
    return logBackends;
}

} // namespace Tests
} // namespace NKikimr
