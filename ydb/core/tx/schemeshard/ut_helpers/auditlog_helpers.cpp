#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>

#include "auditlog_helpers.h"

namespace NSchemeShardUT_Private {

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
