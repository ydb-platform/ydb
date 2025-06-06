#pragma once

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/log.h>
#include <ydb/core/audit/audit_log_service.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>

namespace NKikimr {
namespace Tests {

class TMemoryLogBackend: public TLogBackend {
public:
    std::vector<std::string>& Buffer;

    TMemoryLogBackend(std::vector<std::string>& buffer)
        : Buffer(buffer)
    {}

    virtual void WriteData(const TLogRecord& rec) override {
        Buffer.emplace_back(rec.Data, rec.Len);
    }

    virtual void ReopenLog() override {
    }
};

NAudit::TAuditLogBackends CreateTestAuditLogBackends(std::vector<std::string>& lineBuffer);

} // namespace Tests
} // namespace NKikimr
