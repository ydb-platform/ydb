#pragma once

#include <string>
#include <vector>

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/log.h>
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

} // namespace Tests
} // namespace NKikimr
