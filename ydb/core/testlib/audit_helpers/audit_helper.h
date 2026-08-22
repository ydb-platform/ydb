#pragma once

#include <string>
#include <vector>

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/log.h>
#include <util/datetime/base.h>
#include <ydb/core/audit/audit_log_service.h>

namespace NKikimr {
namespace Tests {

class TMemoryLogBackend: public TLogBackend {
public:
    std::vector<std::string>& Buffer;

    TMemoryLogBackend(std::vector<std::string>& buffer);
    virtual void WriteData(const TLogRecord& rec) override;
    virtual void ReopenLog() override;
};

NAudit::TAuditLogBackends CreateTestAuditLogBackends(std::vector<std::string>& lineBuffer);

std::string FindAuditLine(const std::vector<std::string>& auditLines, const std::string& substr);

// Waits until an audit line containing substr appears in auditLines.
// Returns true and writes the line to outLine on success, false on timeout.
// There is a data race: auditLines is written from another thread/actor without
// synchronization. Same limitation as WaitForAuditLogLines in security/audit_ut.cpp.
bool WaitAndFindAuditLine(
    const std::vector<std::string>& auditLines,
    const std::string& substr,
    std::string& outLine,
    size_t maxAttempts = 50,
    TDuration retryDelay = TDuration::MilliSeconds(100));

} // namespace Tests
} // namespace NKikimr
