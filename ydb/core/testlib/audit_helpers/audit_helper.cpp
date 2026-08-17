#include "audit_helper.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/thread.h>

#include <algorithm>

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

bool WaitAndFindAuditLine(
    const std::vector<std::string>& auditLines,
    const std::string& substr,
    std::string& outLine,
    size_t maxAttempts,
    TDuration retryDelay)
{
    for (size_t attempt = 0; attempt < maxAttempts; ++attempt) {
        auto found = std::find_if(auditLines.rbegin(), auditLines.rend(), [&](const auto& line) {
            return line.contains(substr);
        });
        if (found != auditLines.rend()) {
            outLine = *found;
            Cerr << "AUDIT LOG checked line:" << Endl << "    " << outLine << Endl;
            return true;
        }
        Sleep(retryDelay);
    }
    return false;
}

std::string FindAuditLine(const std::vector<std::string>& auditLines, const std::string& substr) {
    Cerr << "AUDIT LOG buffer(" << auditLines.size() << "):" << Endl;
    for (auto i : auditLines) {
        Cerr << "    " << i << Endl;
    }
    auto found = std::find_if(auditLines.rbegin(), auditLines.rend(), [&](auto i) { return i.contains(substr); });
    UNIT_ASSERT_C(found != auditLines.rend(), "No audit record with substring: '" + substr + "'");
    auto line = *found;
    Cerr << "AUDIT LOG checked line:" << Endl << "    " << line << Endl;
    return line;
}

} // namespace Tests
} // namespace NKikimr
