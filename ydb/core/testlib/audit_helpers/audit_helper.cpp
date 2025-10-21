#include "audit_helper.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace Tests {

TMemoryLogBackend::TMemoryLogBackend(std::vector<std::string>& buffer)
    : Buffer(buffer)
{}

void TMemoryLogBackend::WriteData(const TLogRecord& rec) {
    Buffer.emplace_back(rec.Data, rec.Len);
}

void TMemoryLogBackend::ReopenLog() {
    // nothing
}

NAudit::TAuditLogBackends CreateTestAuditLogBackends(std::vector<std::string>& lineBuffer) {
    NAudit::TAuditLogBackends logBackends;
    logBackends[NKikimrConfig::TAuditConfig::TXT].emplace_back(new TMemoryLogBackend(lineBuffer));
    return logBackends;
}

std::string FindAuditLine(const std::vector<std::string>& auditLines, const std::string& substr) {
    Cerr << "AUDIT LOG buffer(" << auditLines.size() << "):" << Endl;
    for (auto i : auditLines) {
        Cerr << "    " << i << Endl;
    }
    auto found = std::find_if(auditLines.begin(), auditLines.end(), [&](auto i) { return i.contains(substr); });
    UNIT_ASSERT_C(found != auditLines.end(), "No audit record with substring: '" + substr + "'");
    auto line = *found;
    Cerr << "AUDIT LOG checked line:" << Endl << "    " << line << Endl;
    return line;
}

} // namespace Tests
} // namespace NKikimr
