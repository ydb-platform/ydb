#include <ydb/core/mon/audit/audit.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NMonitoring::NAudit;

Y_UNIT_TEST_SUITE(TAuditTest) {
    Y_UNIT_TEST(AuditDisabledWithoutAppData) {
        UNIT_ASSERT(!TAuditCtx::AuditEnabled(NKikimrConfig::TAuditConfig::TLogClassConfig::Completed, NACLibProto::SUBJECT_TYPE_ANONYMOUS));
    }
}
