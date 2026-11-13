#include <ydb/core/cms/console/console_audit.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConsole {

Y_UNIT_TEST_SUITE(TConsoleAuditReplaceDatabaseConfigTransaction) {
    Y_UNIT_TEST(Reason) {
        std::vector<std::string> auditLines;
        NActors::TTestBasicRuntime runtime;
        runtime.AuditLogBackends = Tests::CreateTestAuditLogBackends(auditLines);
        runtime.Initialize(TAppPrepare().Unwrap());

        for (bool success : {false, true}) {
            for (const TString& reason : {TString(), TString("Original diagnostic")}) {
                const auto previousSize = auditLines.size();
                runtime.RunCall([&] {
                    AuditLogReplaceDatabaseConfigTransaction("ipv4:192.0.2.1:1234", "user@builtin", "token",
                        "/Root/database", "old", "new", reason, success);
                    return true;
                });
                runtime.WaitFor("console audit record", [&] {
                    return auditLines.size() > previousSize;
                }, TDuration::Seconds(10));

                const auto line = Tests::FindAuditLine(auditLines, "operation=REPLACE DATABASE CONFIG");
                UNIT_ASSERT_STRING_CONTAINS(line, "component=console");
                UNIT_ASSERT_STRING_CONTAINS(line, success ? "status=SUCCESS" : "status=ERROR");
                if (!reason.empty()) {
                    UNIT_ASSERT_STRING_CONTAINS(line, "reason=Original diagnostic,");
                } else if (!success) {
                    UNIT_ASSERT_STRING_CONTAINS(line, "reason=Database configuration replacement failed,");
                } else {
                    UNIT_ASSERT(line.find("reason=") == std::string::npos);
                }
            }
        }
    }
}

} // namespace NKikimr::NConsole
