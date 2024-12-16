#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/config.pb.h>

#include "auditlog_helpers.h"

namespace NSchemeShardUT_Private {

namespace {

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

}  // anonymous namespace

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

}  // namespace NSchemeShardUT_Private
