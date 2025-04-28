#include "backup_test_fixture.h"

TString TBackupTestFixture::DebugListDir(const TString& path) {
    auto res = YdbSchemeClient().ListDirectory(path).GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    TStringBuilder l;
    for (const auto& entry : res.GetChildren()) {
        if (l) {
            l << ", ";
        }
        l << "\"" << entry.Name << "\"";
    }
    return l;
}
